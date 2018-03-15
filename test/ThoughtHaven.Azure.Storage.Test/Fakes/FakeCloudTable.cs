using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace ThoughtHaven.Azure.Storage.Test.Fakes
{
    public class FakeCloudTable : CloudTable
    {
        public FakeCloudTable(string tableName) :
            base(tableAddress: new Uri($"https://example.com/{tableName}")) { }

        public TableRequestOptions CreateAsync_InputRequestOptions;
        public OperationContext CreateAsync_InputOperationContext;
        public override Task CreateAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.CreateAsync_InputRequestOptions = requestOptions;
            this.CreateAsync_InputOperationContext = operationContext;

            return Task.CompletedTask;
        }

        public TableRequestOptions CreateIfNotExistsAsync_InputRequestOptions;
        public OperationContext CreateIfNotExistsAsync_InputOperationContext;
        public bool CreateIfNotExistsAsync_Output = true;
        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.CreateIfNotExistsAsync_InputRequestOptions = requestOptions;
            this.CreateIfNotExistsAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.CreateIfNotExistsAsync_Output);
        }

        public TableRequestOptions DeleteAsync_InputRequestOptions;
        public OperationContext DeleteAsync_InputOperationContext;
        public override Task DeleteAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.DeleteAsync_InputRequestOptions = requestOptions;
            this.DeleteAsync_InputOperationContext = operationContext;

            return Task.CompletedTask;
        }

        public TableRequestOptions DeleteIfExistsAsync_InputRequestOptions;
        public OperationContext DeleteIfExistsAsync_InputOperationContext;
        public bool DeleteIfExistsAsync_Output = true;
        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions,
            OperationContext operationContext)
        {
            this.DeleteIfExistsAsync_InputRequestOptions = requestOptions;
            this.DeleteIfExistsAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.DeleteIfExistsAsync_Output);
        }

        public TableOperation ExecuteAsync_InputOperation;
        public TableRequestOptions ExecuteAsync_InputRequestOptions;
        public OperationContext ExecuteAsync_InputOperationContext;
        public TableResult ExecuteAsync_Output = new TableResult() { HttpStatusCode = 200 };
        public override Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions, OperationContext operationContext)
        {
            this.ExecuteAsync_InputOperation = operation;
            this.ExecuteAsync_InputRequestOptions = requestOptions;
            this.ExecuteAsync_InputOperationContext = operationContext;

            return Task.FromResult(this.ExecuteAsync_Output);
        }

        public TableBatchOperation ExecuteBatchAsync_InputBatch;
        public TableRequestOptions ExecuteBatchAsync_InputRequestOptions;
        public OperationContext ExecuteBatchAsync_InputOperationContext;
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