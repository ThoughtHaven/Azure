using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using ThoughtHaven.Azure.Cosmos.Table;

namespace ThoughtHaven.Data.Fakes
{
    public class FakeTableEntityStore : TableEntityStore
    {
        public FakeTableEntityStore()
            : base(new CloudTable(new Uri("https://example.com/table")),
                  new TableRequestOptions())
        { }

        public string? Retrieve_InputPartitionKey;
        public string? Retrieve_InputRowKey;
        public DynamicTableEntity? Retrieve_Output = new DynamicTableEntity()
        { PartitionKey = "pk", RowKey = "rk" };
        public override Task<TEntity?> Retrieve<TEntity>(string partitionKey, string rowKey)
            where TEntity : class
        {
            this.Retrieve_InputPartitionKey = partitionKey;
            this.Retrieve_InputRowKey = rowKey;

            return Task.FromResult(this.Retrieve_Output as TEntity);
        }

        public ITableEntity? Insert_InputEntity;
        public override Task Insert<TEntity>(TEntity entity)
        {
            this.Insert_InputEntity = entity;

            return Task.CompletedTask;
        }

        public ITableEntity? Replace_InputEntity;
        public override Task Replace<TEntity>(TEntity entity)
        {
            this.Replace_InputEntity = entity;

            return Task.CompletedTask;
        }

        public ITableEntity? Delete_InputEntity;
        public StorageException? Delete_ExceptionToThrow = null;
        public override Task Delete<TEntity>(TEntity entity)
        {
            this.Delete_InputEntity = entity;

            if (this.Delete_ExceptionToThrow != null) { throw this.Delete_ExceptionToThrow; }

            return Task.CompletedTask;
        }
    }
}