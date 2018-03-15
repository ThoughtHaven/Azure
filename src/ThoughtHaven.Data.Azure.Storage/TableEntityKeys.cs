namespace ThoughtHaven.Data
{
    public class TableEntityKeys
    {
        public string PartitionKey { get; }
        public string RowKey { get; }

        public TableEntityKeys(string partitionKey, string rowKey)
        {
            this.PartitionKey = Guard.NullOrWhiteSpace(nameof(partitionKey), partitionKey);
            this.RowKey = Guard.NullOrWhiteSpace(nameof(rowKey), rowKey);
        }
    }
}