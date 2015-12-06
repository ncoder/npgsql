using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Logging;

namespace Npgsql
{
    static class PoolManager
    {
        /// <summary>
        /// Holds connector pools indexed by their connection strings.
        /// </summary>
        internal static ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool> Pools { get; }

        /// <summary>
        /// Maximum number of possible connections in the pool.
        /// </summary>
        internal const int PoolSizeLimit = 1024;

        static PoolManager()
        {
            Pools = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool>();
        }

        internal static ConnectorPool GetOrAdd(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools.GetOrAdd(connString, cs => new ConnectorPool(cs));
        }

        internal static ConnectorPool Get(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools[connString];
        }
    }

    class ConnectorPool
    {
        internal NpgsqlConnectionStringBuilder ConnectionString;

        /// <summary>
        /// Open connectors waiting to be requested by new connections
        /// </summary>
        internal Stack<NpgsqlConnector> Idle;

        readonly int _max, _min;
        internal int Busy { get; private set; }

        internal Queue<TaskCompletionSource<NpgsqlConnector>> Waiting;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        internal ConnectorPool(NpgsqlConnectionStringBuilder csb)
        {
            if (csb.MaxPoolSize < csb.MinPoolSize)
                throw new ArgumentException($"Connection can't have MaxPoolSize {csb.MaxPoolSize} under MinPoolSize {csb.MinPoolSize}");

            _max = csb.MaxPoolSize;
            _min = csb.MinPoolSize;

            ConnectionString = csb;
            Idle = new Stack<NpgsqlConnector>(_max);
            Waiting = new Queue<TaskCompletionSource<NpgsqlConnector>>();
        }

        internal NpgsqlConnector Allocate(NpgsqlConnection conn, string password, NpgsqlTimeout timeout)
        {
            NpgsqlConnector connector;
            Monitor.Enter(this);

            try
            {
                while (Idle.Count + Busy < _min)
                {
                    connector = new NpgsqlConnector(conn, password);
                    // We need to increment the Busy counter, since ANY connection breakage will
                    // flow through the Releade() method below, so we increment, attempt to Open(),
                    // then decrement immediately, when calling PushIdleConnection()
                    Busy++;
                    connector.Open();
                    PushIdleConnection(connector);
                }
            }
            catch
            {
                // If we failed to allocated even one connection, we can cut it short here,
                // and throw from here instead of continuing with the "normal" allocation below
                if (Idle.Count == 0)
                {
                    throw;
                }
            }


            if (Idle.Count > 0)
            {
                connector = Idle.Pop();
                Busy++;
                Monitor.Exit(this);
                return connector;
            }

            if (Busy >= _max)
            {
                // TODO: Async cancellation
                var tcs = new TaskCompletionSource<NpgsqlConnector>();
                Waiting.Enqueue(tcs);
                Monitor.Exit(this);
                if (!tcs.Task.Wait(timeout.TimeLeft))
                {
                    // Re-lock and check in case the task was set to completed after coming out of the Wait
                    lock (this)
                    {
                        if (!tcs.Task.IsCompleted)
                        {
                            tcs.SetCanceled();
                            throw new TimeoutException($"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {ConnectionString.Timeout} seconds)");
                        }
                    }
                }
                return tcs.Task.Result;
            }

            // No idle connectors are available, and we're under the pool's maximum capacity.
            Busy++;
            Monitor.Exit(this);

            try
            {
                connector = new NpgsqlConnector(conn, password);
                connector.Open();
                return connector;
            }
            catch
            {
                lock (this)
                {
                    Busy--;
                }
                throw;
            }
        }

        void PushIdleConnection(NpgsqlConnector connector)
        {
            connector.Connection = null;
            Idle.Push(connector);
            Busy--;
        }

        internal void Release(NpgsqlConnector connector)
        {
            if (connector.IsBroken)
            {
                lock (this) { Busy--; }
                return;
            }

            connector.Reset();
            lock (this)
            {
                while (Waiting.Count > 0)
                {
                    var tcs = Waiting.Dequeue();
                    if (tcs.TrySetResult(connector)) {
                        return;
                    }
                }
                PushIdleConnection(connector);
            }
        }

        public override string ToString()
        {
            return $"[{Busy} busy, {Idle.Count} idle, {Waiting.Count} waiting]";
        }

        [ContractInvariantMethod]
        void ObjectInvariants()
        {
            Contract.Invariant(Idle.Count <= _max);
            Contract.Invariant(Busy <= _max);
        }
    }
}
