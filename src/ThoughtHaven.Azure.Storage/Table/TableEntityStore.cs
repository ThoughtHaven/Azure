using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ThoughtHaven.Azure.Storage.Table
{
    public class TableEntityStore
    {
        public TableExistenceTester ExistenceTester { get; }
        public CloudTable Table { get; }
        public TableRequestOptions Options { get; }

        public TableEntityStore(CloudTable table, TableRequestOptions options)
            : this(table, new TableExistenceTester(options), options)
        { }

        public TableEntityStore(CloudTable table, TableExistenceTester existenceTester,
            TableRequestOptions options)
        {
            this.ExistenceTester = existenceTester ?? throw new ArgumentNullException(
                nameof(existenceTester));
            this.Table = table ?? throw new ArgumentNullException(nameof(table));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
        }
        
        public virtual async Task<TEntity> Retrieve<TEntity>(string partitionKey, string rowKey)
            where TEntity : class, ITableEntity, new()
        {
            Guard.NullOrWhiteSpace(nameof(partitionKey), partitionKey);
            Guard.NullOrWhiteSpace(nameof(rowKey), rowKey);

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation retrieve = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);

            TableResult result = await this.Table.ExecuteAsync(retrieve, this.Options,
                operationContext: null).ConfigureAwait(false);

            if (result.HttpStatusCode == 404) { return null; }

            result.EnsureSuccessStatusCode();

            return (TEntity)result.Result;
        }

        public virtual async Task Insert<TEntity>(TEntity entity) where TEntity : ITableEntity
        {
            if (entity == null) { throw new ArgumentNullException(nameof(entity)); }

            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.PartitionKey)}.");
            }

            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.RowKey)}.");
            }

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation insert = TableOperation.Insert(entity);

            var result = await this.Table.ExecuteAsync(insert, this.Options,
                operationContext: null).ConfigureAwait(false);

            result.EnsureSuccessStatusCode();
        }

        public virtual async Task InsertOrReplace<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            if (entity == null) { throw new ArgumentNullException(nameof(entity)); }

            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.PartitionKey)}.");
            }

            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.RowKey)}.");
            }

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation operation = TableOperation.InsertOrReplace(entity);

            var result = await this.Table.ExecuteAsync(operation, this.Options,
                operationContext: null).ConfigureAwait(false);

            result.EnsureSuccessStatusCode();
        }

        public virtual async Task Replace<TEntity>(TEntity entity) where TEntity : ITableEntity
        {
            if (entity == null) { throw new ArgumentNullException(nameof(entity)); }

            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.PartitionKey)}.");
            }

            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.RowKey)}.");
            }

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation replace = TableOperation.Replace(entity);

            var result = await this.Table.ExecuteAsync(replace, this.Options,
                operationContext: null).ConfigureAwait(false);

            result.EnsureSuccessStatusCode();
        }

        public virtual async Task Merge<TEntity>(TEntity entity) where TEntity : ITableEntity
        {
            if (entity == null) { throw new ArgumentNullException(nameof(entity)); }

            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.PartitionKey)}.");
            }

            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.RowKey)}.");
            }

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation merge = TableOperation.Merge(entity);

            var result = await this.Table.ExecuteAsync(merge, this.Options,
                operationContext: null).ConfigureAwait(false);

            result.EnsureSuccessStatusCode();
        }

        public virtual async Task Delete<TEntity>(TEntity entity) where TEntity : ITableEntity
        {
            if (entity == null) { throw new ArgumentNullException(nameof(entity)); }

            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.PartitionKey)}.");
            }

            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException(paramName: nameof(entity),
                    message: $"The {nameof(entity)} argument requires a {nameof(entity.RowKey)}.");
            }

            await this.ExistenceTester.EnsureExists(this.Table).ConfigureAwait(false);

            TableOperation delete = TableOperation.Delete(entity);

            var result = await this.Table.ExecuteAsync(delete, this.Options,
                operationContext: null).ConfigureAwait(false);

            result.EnsureSuccessStatusCode();
        }
    }
}