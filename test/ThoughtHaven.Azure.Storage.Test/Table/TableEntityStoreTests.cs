﻿using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using ThoughtHaven.Azure.Storage.Table;
using ThoughtHaven.Azure.Storage.Test.Fakes;
using Xunit;

namespace ThoughtHaven.Azure.Storage.Test.Table
{
    public class TableEntityStoreTests
    {
        public class Constructor
        {
            public class TableAndOptionsOverload
            {
                [Fact]
                public void NullTable_Throws()
                {
                    Assert.Throws<ArgumentNullException>("table", () =>
                    {
                        new TableEntityStore(
                            table: null,
                            options: Options());
                    });
                }

                [Fact]
                public void NullOptions_Throws()
                {
                    Assert.Throws<ArgumentNullException>("options", () =>
                    {
                        new TableEntityStore(
                            table: Table(),
                            options: null);
                    });
                }

                [Fact]
                public void WhenCalled_SetsTable()
                {
                    var table = Table();
                    var options = Options();

                    var store = new TableEntityStore(table, options);

                    Assert.Equal(table, store.Table);
                }

                [Fact]
                public void WhenCalled_SetsExistenceTester()
                {
                    var table = Table();
                    var options = Options();

                    var store = new TableEntityStore(table, options);

                    Assert.NotNull(store.ExistenceTester);
                }

                [Fact]
                public void WhenCalled_SetsOptions()
                {
                    var table = Table();
                    var options = Options();

                    var store = new TableEntityStore(table, options);

                    Assert.Equal(options, store.Options);
                }
            }

            public class TableAndExistenceTesterAndOptionsOverload
            {
                [Fact]
                public void NullTable_Throws()
                {
                    Assert.Throws<ArgumentNullException>("table", () =>
                    {
                        new TableEntityStore(
                            table: null,
                            existenceTester: Tester(),
                            options: Options());
                    });
                }

                [Fact]
                public void NullExistenceTester_Throws()
                {
                    Assert.Throws<ArgumentNullException>("existenceTester", () =>
                    {
                        new TableEntityStore(
                            table: Table(),
                            existenceTester: null,
                            options: Options());
                    });
                }

                [Fact]
                public void NullOptions_Throws()
                {
                    Assert.Throws<ArgumentNullException>("options", () =>
                    {
                        new TableEntityStore(
                            table: Table(),
                            existenceTester: Tester(),
                            options: null);
                    });
                }

                [Fact]
                public void WhenCalled_SetsTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();

                    var store = new TableEntityStore(table, tester, options);

                    Assert.Equal(table, store.Table);
                }

                [Fact]
                public void WhenCalled_SetsExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();

                    var store = new TableEntityStore(table, tester, options);

                    Assert.Equal(tester, store.ExistenceTester);
                }

                [Fact]
                public void WhenCalled_SetsOptions()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();

                    var store = new TableEntityStore(table, tester, options);

                    Assert.Equal(options, store.Options);
                }
            }
        }

        public class RetrieveMethod
        {
            public class TEntityGeneric
            {
                public class PartitionKeyAndRowKeyOverload
                {
                    [Fact]
                    public async Task NullPartitionKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentNullException>("partitionKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: null,
                                rowKey: "rk");
                        });
                    }

                    [Fact]
                    public async Task EmptyPartitionKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentException>("partitionKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: "",
                                rowKey: "rk");
                        });
                    }

                    [Fact]
                    public async Task WhiteSpacePartitionKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentException>("partitionKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: " ",
                                rowKey: "rk");
                        });
                    }

                    [Fact]
                    public async Task NullRowKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentNullException>("rowKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: "pk",
                                rowKey: null);
                        });
                    }

                    [Fact]
                    public async Task EmptyRowKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentException>("rowKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: "pk",
                                rowKey: "");
                        });
                    }

                    [Fact]
                    public async Task WhiteSpaceRowKey_Throws()
                    {
                        await Assert.ThrowsAsync<ArgumentException>("rowKey", async () =>
                        {
                            await Store().Retrieve<DynamicTableEntity>(
                                partitionKey: "pk",
                                rowKey: " ");
                        });
                    }

                    [Fact]
                    public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                    {
                        var table = Table();
                        var tester = Tester();
                        var options = Options();

                        await Store(table, tester, options).Retrieve<DynamicTableEntity>(
                            "pk", "rk");

                        Assert.Equal(table, tester.EnsureExists_InputTable);
                    }

                    [Fact]
                    public async Task WhenCalled_CallsExecuteOnTable()
                    {
                        var table = Table();
                        var tester = Tester();
                        var options = Options();

                        await Store(table, tester, options).Retrieve<DynamicTableEntity>(
                            "pk", "rk");

                        Assert.Equal(TableOperationType.Retrieve,
                            table.ExecuteAsync_InputOperation.OperationType);
                        Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    }

                    [Fact]
                    public async Task ExecuteOnTableReturns404StatusCode_ReturnsNull()
                    {
                        var table = Table();
                        table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 404 };
                        var tester = Tester();
                        var options = Options();

                        var result = await Store(table, tester, options).Retrieve<DynamicTableEntity>(
                            "pk", "rk");

                        Assert.Null(result);
                    }

                    [Fact]
                    public async Task ExecuteOnTableReturnsErrorStatusCode_Throws()
                    {
                        var table = Table();
                        table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                        var tester = Tester();
                        var options = Options();

                        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                        {
                            await Store(table, tester, options).Retrieve<DynamicTableEntity>(
                                "pk", "rk");
                        });
                    }

                    [Fact]
                    public async Task WhenCalled_ReturnsResult()
                    {
                        var table = Table();
                        var tester = Tester();
                        var options = Options();

                        var result = await Store(table, tester, options).Retrieve<DynamicTableEntity>(
                            "pk", "rk");

                        Assert.Equal(table.ExecuteAsync_Output.Result, result);
                    }
                }
            }
        }

        public class InsertMethod
        {
            public class EntityOverload
            {
                [Fact]
                public async Task NullEntity_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("entity", async () =>
                    {
                        await Store().Insert<TableEntity>(entity: null);
                    });
                }

                [Fact]
                public async Task NullEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity(null, "rk"));
                    });
                }

                [Fact]
                public async Task EmptyEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity("", "rk"));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity(" ", "rk"));
                    });
                }

                [Fact]
                public async Task NullEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity("pk", null));
                    });
                }

                [Fact]
                public async Task EmptyEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity("pk", ""));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Insert(new TableEntity("pk", " "));
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Insert(entity);

                    Assert.Equal(table, tester.EnsureExists_InputTable);
                }

                [Fact]
                public async Task WhenCalled_CallsExecuteOnTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Insert(entity);

                    Assert.Equal(TableOperationType.Insert,
                        table.ExecuteAsync_InputOperation.OperationType);
                    Assert.Equal(entity, table.ExecuteAsync_InputOperation.Entity);
                    Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    Assert.Null(table.ExecuteAsync_InputOperationContext);
                }

                [Fact]
                public async Task ExecuteOnTableReturnsErrorHttpCode_Throws()
                {
                    var table = Table();
                    table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await Store(table, tester, options).Insert(entity);
                    });
                }
            }
        }

        public class InsertOrReplaceMethod
        {
            public class EntityOverload
            {
                [Fact]
                public async Task NullEntity_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("entity", async () =>
                    {
                        await Store().InsertOrReplace<TableEntity>(entity: null);
                    });
                }

                [Fact]
                public async Task NullEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity(null, "rk"));
                    });
                }

                [Fact]
                public async Task EmptyEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity("", "rk"));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity(" ", "rk"));
                    });
                }

                [Fact]
                public async Task NullEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity("pk", null));
                    });
                }

                [Fact]
                public async Task EmptyEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity("pk", ""));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().InsertOrReplace(new TableEntity("pk", " "));
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).InsertOrReplace(entity);

                    Assert.Equal(table, tester.EnsureExists_InputTable);
                }

                [Fact]
                public async Task WhenCalled_CallsExecuteOnTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).InsertOrReplace(entity);

                    Assert.Equal(TableOperationType.InsertOrReplace,
                        table.ExecuteAsync_InputOperation.OperationType);
                    Assert.Equal(entity, table.ExecuteAsync_InputOperation.Entity);
                    Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    Assert.Null(table.ExecuteAsync_InputOperationContext);
                }

                [Fact]
                public async Task ExecuteOnTableReturnsErrorHttpCode_Throws()
                {
                    var table = Table();
                    table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await Store(table, tester, options).InsertOrReplace(entity);
                    });
                }
            }
        }

        public class ReplaceMethod
        {
            public class EntityOverload
            {
                [Fact]
                public async Task NullEntity_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("entity", async () =>
                    {
                        await Store().Replace<TableEntity>(entity: null);
                    });
                }

                [Fact]
                public async Task NullEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity(null, "rk"));
                    });
                }

                [Fact]
                public async Task EmptyEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity("", "rk"));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity(" ", "rk"));
                    });
                }

                [Fact]
                public async Task NullEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity("pk", null));
                    });
                }

                [Fact]
                public async Task EmptyEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity("pk", ""));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Replace(new TableEntity("pk", " "));
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Replace(entity);

                    Assert.Equal(table, tester.EnsureExists_InputTable);
                }

                [Fact]
                public async Task WhenCalled_CallsExecuteOnTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Replace(entity);

                    Assert.Equal(TableOperationType.Replace,
                        table.ExecuteAsync_InputOperation.OperationType);
                    Assert.Equal(entity, table.ExecuteAsync_InputOperation.Entity);
                    Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    Assert.Null(table.ExecuteAsync_InputOperationContext);
                }

                [Fact]
                public async Task ExecuteOnTableReturnsErrorHttpCode_Throws()
                {
                    var table = Table();
                    table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await Store(table, tester, options).Replace(entity);
                    });
                }
            }
        }

        public class MergeMethod
        {
            public class EntityOverload
            {
                [Fact]
                public async Task NullEntity_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("entity", async () =>
                    {
                        await Store().Merge<TableEntity>(entity: null);
                    });
                }

                [Fact]
                public async Task NullEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity(null, "rk"));
                    });
                }

                [Fact]
                public async Task EmptyEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity("", "rk"));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity(" ", "rk"));
                    });
                }

                [Fact]
                public async Task NullEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity("pk", null));
                    });
                }

                [Fact]
                public async Task EmptyEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity("pk", ""));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Merge(new TableEntity("pk", " "));
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Merge(entity);

                    Assert.Equal(table, tester.EnsureExists_InputTable);
                }

                [Fact]
                public async Task WhenCalled_CallsExecuteOnTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Merge(entity);

                    Assert.Equal(TableOperationType.Merge,
                        table.ExecuteAsync_InputOperation.OperationType);
                    Assert.Equal(entity, table.ExecuteAsync_InputOperation.Entity);
                    Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    Assert.Null(table.ExecuteAsync_InputOperationContext);
                }

                [Fact]
                public async Task ExecuteOnTableReturnsErrorHttpCode_Throws()
                {
                    var table = Table();
                    table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await Store(table, tester, options).Merge(entity);
                    });
                }
            }
        }

        public class DeleteMethod
        {
            public class EntityOverload
            {
                [Fact]
                public async Task NullEntity_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("entity", async () =>
                    {
                        await Store().Delete<TableEntity>(entity: null);
                    });
                }

                [Fact]
                public async Task NullEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity(null, "rk"));
                    });
                }

                [Fact]
                public async Task EmptyEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity("", "rk"));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityPartitionKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity(" ", "rk"));
                    });
                }

                [Fact]
                public async Task NullEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity("pk", null));
                    });
                }

                [Fact]
                public async Task EmptyEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity("pk", ""));
                    });
                }

                [Fact]
                public async Task WhiteSpaceEntityRowKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentException>("entity", async () =>
                    {
                        await Store().Delete(new TableEntity("pk", " "));
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsEnsureExistsOnExistenceTester()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Delete(entity);

                    Assert.Equal(table, tester.EnsureExists_InputTable);
                }

                [Fact]
                public async Task WhenCalled_CallsExecuteOnTable()
                {
                    var table = Table();
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Store(table, tester, options).Delete(entity);

                    Assert.Equal(TableOperationType.Delete,
                        table.ExecuteAsync_InputOperation.OperationType);
                    Assert.Equal(entity, table.ExecuteAsync_InputOperation.Entity);
                    Assert.Equal(options, table.ExecuteAsync_InputRequestOptions);
                    Assert.Null(table.ExecuteAsync_InputOperationContext);
                }

                [Fact]
                public async Task ExecuteOnTableReturnsErrorHttpCode_Throws()
                {
                    var table = Table();
                    table.ExecuteAsync_Output = new TableResult() { HttpStatusCode = 300 };
                    var tester = Tester();
                    var options = Options();
                    var entity = Entity();

                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await Store(table, tester, options).Delete(entity);
                    });
                }
            }
        }

        private static FakeCloudTable Table() => new FakeCloudTable("table");
        private static FakeTableExistenceTester Tester() => new FakeTableExistenceTester();
        private static TableRequestOptions Options() => new TableRequestOptions();
        private static TableEntityStore Store(FakeCloudTable table = null,
            FakeTableExistenceTester tester = null, TableRequestOptions options = null) =>
            new TableEntityStore(table ?? Table(), tester ?? Tester(), options ?? Options());
        private static TableEntity Entity() => new TableEntity()
        {
            PartitionKey = "pk",
            RowKey = "rk",
            ETag = "*",
        };
    }
}