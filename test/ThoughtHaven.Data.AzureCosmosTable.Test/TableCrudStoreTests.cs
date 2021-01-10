using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using ThoughtHaven.Data.Fakes;
using Xunit;

namespace ThoughtHaven.Data
{
    public class TableCrudStoreTests
    {
        public class Constructor
        {
            public class PrimaryOverload
            {
                [Fact]
                public void NullEntityStore_Throws()
                {
                    Assert.Throws<ArgumentNullException>("entityStore", () =>
                    {
                        new TableCrudStore<int?, object>(
                            entityStore: null!,
                            dataKeyToEntityKeys: DataKeyToEntityKeys(),
                            dataToEntityKeys: DataToEntityKeys());
                    });
                }

                [Fact]
                public void NullDataKeyToEntityKeys_Throws()
                {
                    Assert.Throws<ArgumentNullException>("dataKeyToEntityKeys", () =>
                    {
                        new TableCrudStore<int?, object>(
                            entityStore: EntityStore(),
                            dataKeyToEntityKeys: null!,
                            dataToEntityKeys: DataToEntityKeys());
                    });
                }

                [Fact]
                public void NullDataToEntityKeys_Throws()
                {
                    Assert.Throws<ArgumentNullException>("dataToEntityKeys", () =>
                    {
                        new TableCrudStore<int?, object>(
                            entityStore: EntityStore(),
                            dataKeyToEntityKeys: DataKeyToEntityKeys(),
                            dataToEntityKeys: null!);
                    });
                }
            }
        }

        public class RetrieveMethod
        {
            public class KeyOverload
            {
                [Fact]
                public async Task NullKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("key", async () =>
                    {
                        await CrudStore().Retrieve(key: null);
                    });
                }

                [Fact]
                public async Task WhenCalled_RetrieveOnEntityStore()
                {
                    var store = EntityStore();

                    await CrudStore(store).Retrieve(key: 1);

                    Assert.Equal("pk", store.Retrieve_InputPartitionKey);
                    Assert.Equal("rk", store.Retrieve_InputRowKey);
                }

                [Fact]
                public async Task RetrieveOnEntityStoreReturnsNull_ReturnsNull()
                {
                    var store = EntityStore();
                    store.Retrieve_Output = null;

                    var result = await CrudStore(store).Retrieve(key: 1);

                    Assert.Null(result);
                }

                [Fact]
                public async Task WhenCalled_ReturnData()
                {
                    var store = EntityStore();
                    store.Retrieve_Output!.Properties.Add(nameof(DataModel.Id),
                        EntityProperty.GeneratePropertyForString("id"));

                    var result = await CrudStore(store).Retrieve(key: 1);

                    Assert.Equal("id", result!.Id);
                }
            }
        }

        public class CreateMethod
        {
            public class DataOverload
            {
                [Fact]
                public async Task NullData_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("data", async () =>
                    {
                        await CrudStore().Create(data: null!);
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsInsertOnEntityStore()
                {
                    var entityStore = EntityStore();
                    var data = Data();

                    await CrudStore(entityStore).Create(data);

                    Assert.Equal("pk", entityStore.Insert_InputEntity!.PartitionKey);
                    Assert.Equal("rk", entityStore.Insert_InputEntity.RowKey);
                    Assert.Equal("*", entityStore.Insert_InputEntity.ETag);
                    Assert.Equal(data.Id,
                        ((DynamicTableEntity)entityStore.Insert_InputEntity).Properties.Values
                            .First().StringValue);
                }

                [Fact]
                public async Task WhenCalled_ReturnsData()
                {
                    var data = Data();

                    var result = await CrudStore().Create(data);

                    Assert.Equal(data, result);
                }
            }
        }

        public class UpdateMethod
        {
            public class DataOverload
            {
                [Fact]
                public async Task NullData_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("data", async () =>
                    {
                        await CrudStore().Update(data: null!);
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsReplaceOnEntityStore()
                {
                    var entityStore = EntityStore();
                    var data = Data();

                    await CrudStore(entityStore).Update(data);

                    Assert.Equal("pk", entityStore.Replace_InputEntity!.PartitionKey);
                    Assert.Equal("rk", entityStore.Replace_InputEntity.RowKey);
                    Assert.Equal("*", entityStore.Replace_InputEntity.ETag);
                    Assert.Equal(data.Id,
                        ((DynamicTableEntity)entityStore.Replace_InputEntity).Properties.Values
                            .First().StringValue);
                }

                [Fact]
                public async Task WhenCalled_ReturnsData()
                {
                    var data = Data();

                    var result = await CrudStore().Update(data);

                    Assert.Equal(data, result);
                }
            }
        }

        public class DeleteMethod
        {
            public class DataOverload
            {
                [Fact]
                public async Task NullKey_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("key", async () =>
                    {
                        await CrudStore().Delete(key: null);
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsDeleteOnEntityStore()
                {
                    var entityStore = EntityStore();

                    await CrudStore(entityStore).Delete(key: 1);

                    Assert.Equal("pk", entityStore.Delete_InputEntity!.PartitionKey);
                    Assert.Equal("rk", entityStore.Delete_InputEntity.RowKey);
                    Assert.Equal("*", entityStore.Delete_InputEntity.ETag);
                }

                [Fact]
                public async Task DeleteOnEntityStoreThrowsStorageException_Throws()
                {
                    var entityStore = EntityStore();
                    entityStore.Delete_ExceptionToThrow = StorageException(500);

                    await Assert.ThrowsAsync<StorageException>(async () =>
                    {
                        await CrudStore(entityStore).Delete(key: 1);
                    });
                }

                [Fact]
                public async Task DeleteOnEntityStoreThrowsStorageExceptionWith404StatusCode_Swallows()
                {
                    var entityStore = EntityStore();
                    entityStore.Delete_ExceptionToThrow = StorageException(404);

                    await CrudStore(entityStore).Delete(key: 1);
                }
            }
        }

        private static FakeTableEntityStore EntityStore() => new FakeTableEntityStore();
        private static Func<int?, TableEntityKeys> DataKeyToEntityKeys() =>
            key => new TableEntityKeys("pk", "rk");
        private static Func<object, TableEntityKeys> DataToEntityKeys() =>
            data => new TableEntityKeys("pk", "rk");
        private static TableCrudStore<int?, DataModel> CrudStore(
            FakeTableEntityStore? entityStore = null,
            Func<int?, TableEntityKeys>? dataKeyToEntityKeys = null,
            Func<object, TableEntityKeys>? dataToEntityKeys = null) =>
            new TableCrudStore<int?, DataModel>(entityStore ?? EntityStore(),
                dataKeyToEntityKeys ?? DataKeyToEntityKeys(),
                dataToEntityKeys ?? DataToEntityKeys());
        private static DataModel Data() => new DataModel() { Id = "id" };
        private static StorageException StorageException(int statusCode) =>
            new StorageException(new RequestResult() { HttpStatusCode = statusCode },
                "message", new Exception());
        private class DataModel
        {
            public string? Id { get; set; }
        }
    }
}