using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ThoughtHaven.Azure.Storage.Table;

namespace ThoughtHaven.Data
{
    public class TableCrudStore<TKey, TData> : ICrudStore<TKey, TData>
        where TData : class, new()
    {
        protected TableEntityStore EntityStore { get; }
        protected Func<TKey, TableEntityKeys> DataKeyToEntityKeys { get; }
        protected Func<TData, TableEntityKeys> DataToEntityKeys { get; }

        public TableCrudStore(TableEntityStore entityStore,
            Func<TKey, TableEntityKeys> dataKeyToEntityKeys,
            Func<TData, TableEntityKeys> dataToEntityKeys)
        {
            this.EntityStore = Guard.Null(nameof(entityStore), entityStore);
            this.DataKeyToEntityKeys = Guard.Null(nameof(dataKeyToEntityKeys),
                dataKeyToEntityKeys);
            this.DataToEntityKeys = Guard.Null(nameof(dataToEntityKeys), dataToEntityKeys);
        }

        public virtual async Task<TData> Retrieve(TKey key)
        {
            Guard.Null(nameof(key), key);

            var keys = this.DataKeyToEntityKeys(key);

            var entity = await this.EntityStore.Retrieve<DynamicTableEntity>(
                keys.PartitionKey, keys.RowKey).ConfigureAwait(false);

            if (entity == null) { return null; }

            return this.Convert(entity);
        }

        public virtual async Task<TData> Create(TData data)
        {
            Guard.Null(nameof(data), data);

            var entity = this.Convert(data);

            await this.EntityStore.Insert(entity).ConfigureAwait(false);

            return data;
        }

        public virtual async Task<TData> Update(TData data)
        {
            Guard.Null(nameof(data), data);

            var entity = this.Convert(data);

            await this.EntityStore.Replace(entity).ConfigureAwait(false);

            return data;
        }

        public virtual async Task Delete(TKey key)
        {
            Guard.Null(nameof(key), key);

            var keys = this.DataKeyToEntityKeys(key);

            var entity = new TableEntity(keys.PartitionKey, keys.RowKey) { ETag = "*" };

            try
            {
                await this.EntityStore.Delete(entity);
            }
            catch (StorageException e) when (e.RequestInformation.HttpStatusCode == 404)
            { return; }
        }

        protected virtual TData Convert(DynamicTableEntity entity)
        {
            Guard.Null(nameof(entity), entity);

            var data = new TData();
            TableEntity.ReadUserObject(data, entity.Properties, operationContext: null);
            return data;
        }

        protected virtual ITableEntity Convert(TData data)
        {
            Guard.Null(nameof(data), data);

            var keys = this.DataToEntityKeys(data);
            var properties = TableEntity.WriteUserObject(data, operationContext: null);

            return new DynamicTableEntity(keys.PartitionKey, keys.RowKey)
            {
                ETag = "*",
                Properties = properties,
            };
        }
    }
}