#region License
// The PostgreSQL License
//
// Copyright (C) 2015 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NUnit.Framework;

namespace Npgsql.Tests
{
    class ConnectionPoolTests : TestBase
    {
        [Test]
        public void MinPoolSizeEqualsMaxPoolSize()
        {
            using (var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ConnectionString) {
                MinPoolSize = 30,
                MaxPoolSize = 30
            }))
            {
                conn.Open();
            }
        }

        [Test]
        public void MinPoolSizeLargeThanMaxPoolSize()
        {
            using (var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                MinPoolSize = 2,
                MaxPoolSize = 1
            }))
            {
                Assert.That(() => conn.Open(), Throws.Exception.TypeOf<ArgumentException>());
            }
        }

        [Test]
        public void MinPoolSizeLargeThanPoolSizeLimit()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString);
            Assert.That(() => csb.MinPoolSize = PoolManager.PoolSizeLimit + 1, Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        }

        [Test]
        public void MinPoolSize()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) { MinPoolSize = 2 };
            using (var conn = new NpgsqlConnection(connString))
            {
                connString = conn.Settings; // Shouldn't be necessary
                conn.Open();
                conn.Close();
            }

            var pool = PoolManager.Pools[connString];
            Assert.That(pool.Idle, Has.Count.EqualTo(2));
            /*
            using (var conn1 = new NpgsqlConnection(connString))
            using (var conn2 = new NpgsqlConnection(connString))
            using (var conn3 = new NpgsqlConnection(connString))
            {
                conn1.Open(); conn2.Open(); conn3.Open();
            }
            Assert.That(pool.Idle, Has.Count.EqualTo(2));*/
        }

        [Test, Description("Broken connection(s) leaves pool in consistent state")]
        public void ConnectionsAreDistinct()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) {MinPoolSize = 3};

            using (var conn1 = new NpgsqlConnection(connString))
            using (var conn2 = new NpgsqlConnection(connString))
            {
                conn1.Open();
                conn2.Open();
                Assert.That(conn1.Connector, Is.Not.SameAs(conn2.Connector));
                Assert.That(conn1, Is.SameAs(conn1.Connector.Connection));
                Assert.That(conn2, Is.SameAs(conn2.Connector.Connection));
            }
        }

        [Test, Description("Broken connection(s) leaves pool in consistent state")]
        public void MinPoolSizeIsRespectedAfterBrokenConnections()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) { MinPoolSize = 3 };

            using (var conn1 = new NpgsqlConnection(connString))
            using (var conn2 = new NpgsqlConnection(connString))
            using (var conn3 = new NpgsqlConnection(connString))
            {
                conn1.Open(); conn2.Open(); conn3.Open();

                var pool = PoolManager.Pools[conn1.Settings];
                Assert.That(pool.Busy, Is.EqualTo(3));
                ExecuteNonQuery($"SELECT pg_terminate_backend({conn1.ProcessID})");
                ExecuteNonQuery($"SELECT pg_terminate_backend({conn2.ProcessID})");

                Assert.That(() => ExecuteScalar("select 1", conn1), Throws.Exception.TypeOf<IOException>());
                Assert.That(() => ExecuteScalar("select 1", conn2), Throws.Exception.TypeOf<IOException>());

                Assert.That(pool.Busy, Is.EqualTo(1));
                Assert.That(pool.Idle, Has.Count.EqualTo(0));

                conn1.Open();

                Assert.That(pool.Busy, Is.EqualTo(2));
                Assert.That(pool.Idle, Has.Count.EqualTo(1));
            }
        }


        [Test]
        public void ReuseConnectorBeforeCreatingNew()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                var backendId = conn.Connector.BackendProcessId;
                conn.Close();
                conn.Open();
                Assert.That(conn.Connector.BackendProcessId, Is.EqualTo(backendId));
            }
        }

        [Test]
        public void GetConnectorFromExhaustedPool()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) {
                MaxPoolSize = 1,
                Timeout = 0
            };

            using (var conn1 = new NpgsqlConnection(connString))
            {
                conn1.Open();
                var backendId = conn1.Connector.BackendProcessId;
                // Pool is exhausted
                using (var conn2 = new NpgsqlConnection(connString))
                {
                    new Timer(o => conn1.Close(), null, 1000, Timeout.Infinite);
                    conn2.Open();
                    Assert.That(conn2.Connector.BackendProcessId, Is.EqualTo(backendId));
                }
            }
        }

        [Test]
        public void TimeoutGettingConnectorFromExhaustedPool()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) {
                MaxPoolSize = 1,
                Timeout = 1
            };

            int backendId;
            using (var conn1 = new NpgsqlConnection(connString))
            {
                conn1.Open();
                backendId = conn1.Connector.BackendProcessId;
                // Pool is exhausted
                using (var conn2 = new NpgsqlConnection(connString))
                {
                    Assert.That(() => conn2.Open(), Throws.Exception.TypeOf<TimeoutException>());
                }
            }
            // conn1 should now be back in the pool as idle
            using (var conn3 = new NpgsqlConnection(connString))
            {
                conn3.Open();
                Assert.That(conn3.Connector.BackendProcessId, Is.EqualTo(backendId));
            }
        }

        [Test, Description("Makes sure that when a pooled connection is closed it's properly reset, and that parameter settings aren't leaked")]
        public void ResetOnClose()
        {
            using (var conn = new NpgsqlConnection(ConnectionString + ";SearchPath=public"))
            {
                conn.Open();
                Assert.That(ExecuteScalar("SHOW search_path", conn), Is.Not.StringContaining("pg_temp"));
                var backendId = conn.Connector.BackendProcessId;
                ExecuteNonQuery("SET search_path=pg_temp", conn);
                conn.Close();

                conn.Open();
                Assert.That(conn.Connector.BackendProcessId, Is.EqualTo(backendId));
                Assert.That(ExecuteScalar("SHOW search_path", conn), Is.EqualTo("public"));
            }
        }

        [Test, Description("Connection failure leaves pool in consistent state")]
        public void ConnectionFailure()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString) { Port = 44444 };

            using (var conn = new NpgsqlConnection(csb))
            {
                Assert.That(() => conn.Open(), Throws.Exception.TypeOf<SocketException>());

                var pool = PoolManager.Pools[conn.Settings];
                Assert.That(pool.Busy, Is.EqualTo(0));
                Assert.That(pool.Idle, Has.Count.EqualTo(0));
            }
        }

        [Test, Description("Broken connection(s) leaves pool in consistent state")]
        public void BrokenConnectionGetsCleanedUp()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                var pool = PoolManager.Pools[conn.Settings];
                Assert.That(pool.Busy, Is.EqualTo(2));
                // Use another connection to kill our connector
                var connectorId = conn.ProcessID;
                ExecuteNonQuery($"SELECT pg_terminate_backend({connectorId})");

                // Make sure that npgsql "understands" the connection is broken
                Assert.That(() => ExecuteScalar("select 1", conn), Throws.Exception.TypeOf<IOException>());

                Assert.That(pool.Busy, Is.EqualTo(1));
                Assert.That(pool.Idle, Has.Count.EqualTo(0));
            }
        }

        public ConnectionPoolTests(string backendVersion) : base(backendVersion) {}
    }
}

