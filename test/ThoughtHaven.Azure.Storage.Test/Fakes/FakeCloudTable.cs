using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ThoughtHaven.Azure.Storage.Test.Fakes
{
    public class FakeCloudTable : CloudTable
    {
        public FakeCloudTable(string tableName) :
            base(tableAddress: new Uri($"https://example.com/{tableName}"))
        { }

        public TableQuery<DynamicTableEntity>? ExecuteQuerySegmentedAsync_InputQuery;
        public TableContinuationToken? ExecuteQuerySegmentedAsync_InputToken;
        public TableRequestOptions? ExecuteQuerySegmentedAsync_InputRequestOptions;
        public OperationContext? ExecuteQuerySegmentedAsync_InputOperationContext;
        private bool ExecuteQuerySegmentedAsync_OutputTokenSet = false;
        public TableContinuationToken? ExecuteQuerySegmentedAsync_OutputToken = null;
        public List<DynamicTableEntity> ExecuteQuerySegmentedAsync_OutputEntities = new List<DynamicTableEntity>();
        public List<DynamicTableEntity>? ExecuteQuerySegmentedAsync_OutputContinuationEntities;
        public TableQuerySegment<DynamicTableEntity> ExecuteQuerySegmentedAsync_Output
        {
            get
            {
                var ctor = typeof(TableQuerySegment<DynamicTableEntity>)
                    .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
                    .FirstOrDefault(c => c.GetParameters().Count() == 1);

                var entities = !this.ExecuteQuerySegmentedAsync_OutputTokenSet
                    ? this.ExecuteQuerySegmentedAsync_OutputEntities
                    : this.ExecuteQuerySegmentedAsync_OutputContinuationEntities;

                var segment = (TableQuerySegment<DynamicTableEntity>)ctor.Invoke(
                    new object[] { entities! });

                if (this.ExecuteQuerySegmentedAsync_OutputToken != null &&
                    !this.ExecuteQuerySegmentedAsync_OutputTokenSet)
                {
                    var token = segment.GetType().GetField("continuationToken",
                        BindingFlags.Instance | BindingFlags.NonPublic)!;

                    token.SetValue(segment, this.ExecuteQuerySegmentedAsync_OutputToken);

                    this.ExecuteQuerySegmentedAsync_OutputTokenSet = true;
                }

                return segment;
            }
        }
        public override Task<TableQuerySegment<T>> ExecuteQuerySegmentedAsync<T>(
            TableQuery<T> query, TableContinuationToken token,
            TableRequestOptions requestOptions, OperationContext operationContext)
        {
            this.ExecuteQuerySegmentedAsync_InputQuery = query as TableQuery<DynamicTableEntity>;
            this.ExecuteQuerySegmentedAsync_InputToken = token;
            this.ExecuteQuerySegmentedAsync_InputRequestOptions = requestOptions;
            this.ExecuteQuerySegmentedAsync_InputOperationContext = operationContext;

            return Task.FromResult<TableQuerySegment<T>>(
                (this.ExecuteQuerySegmentedAsync_Output as TableQuerySegment<T>)!);
        }

        public TableRequestOptions? CreateAsync_InputRequestOptions;
        public OperationContext? CreateAsync_InputOperationContext;
        public override Task CreateAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.CreateAsync_InputRequestOptions = requestOptions;
            this.CreateAsync_InputOperationContext = operationContext;

            return Task.CompletedTask;
        }

        public TableRequestOptions? CreateIfNotExistsAsync_InputRequestOptions;
        public OperationContext? CreateIfNotExistsAsync_InputOperationContext;
        public bool CreateIfNotExistsAsync_Output = true;
        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.CreateIfNotExistsAsync_InputRequestOptions = requestOptions;
            this.CreateIfNotExistsAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.CreateIfNotExistsAsync_Output);
        }

        public TableRequestOptions? DeleteAsync_InputRequestOptions;
        public OperationContext? DeleteAsync_InputOperationContext;
        public override Task DeleteAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.DeleteAsync_InputRequestOptions = requestOptions;
            this.DeleteAsync_InputOperationContext = operationContext;

            return Task.CompletedTask;
        }

        public TableRequestOptions? DeleteIfExistsAsync_InputRequestOptions;
        public OperationContext? DeleteIfExistsAsync_InputOperationContext;
        public bool DeleteIfExistsAsync_Output = true;
        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.DeleteIfExistsAsync_InputRequestOptions = requestOptions;
            this.DeleteIfExistsAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.DeleteIfExistsAsync_Output);
        }

        public TableOperation? ExecuteAsync_InputOperation;
        public TableRequestOptions? ExecuteAsync_InputRequestOptions;
        public OperationContext? ExecuteAsync_InputOperationContext;
        public TableResult ExecuteAsync_Output = new TableResult() { HttpStatusCode = 200 };
        public override Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions, OperationContext operationContext)
        {
            this.ExecuteAsync_InputOperation = operation;
            this.ExecuteAsync_InputRequestOptions = requestOptions;
            this.ExecuteAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.ExecuteAsync_Output);
        }

        public TableBatchOperation? ExecuteBatchAsync_InputBatch;
        public TableRequestOptions? ExecuteBatchAsync_InputRequestOptions;
        public OperationContext? ExecuteBatchAsync_InputOperationContext;
        public IList<TableResult> ExecuteBatchAsync_Output = new List<TableResult>()
        { new TableResult() { HttpStatusCode = 200 } };
        public override Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch,
            TableRequestOptions requestOptions, OperationContext operationContext)
        {
            this.ExecuteBatchAsync_InputBatch = batch;
            this.ExecuteBatchAsync_InputRequestOptions = requestOptions;
            this.ExecuteBatchAsync_InputOperationContext = operationContext;


            return Task.FromResult(this.ExecuteBatchAsync_Output);
        }
    }
}