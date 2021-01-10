using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace ThoughtHaven.Azure.Cosmos.Table.Fakes
{
    public class FakeTableExistenceTester : TableExistenceTester
    {
        public TableRequestOptions Options { get; }

        public FakeTableExistenceTester() : this(new TableRequestOptions()) { }

        private FakeTableExistenceTester(TableRequestOptions options) : base(options)
        {
            this.Options = options;
        }

        public CloudTable? EnsureExists_InputTable;
        public override Task EnsureExists(CloudTable table)
        {
            this.EnsureExists_InputTable = table;

            return base.EnsureExists(table);
        }

        public CloudTable? GetStorageUri_InputTable;
        public StorageUri? GetStorageUri_Output = null;
        new public StorageUri GetStorageUri(CloudTable table)
        {
            this.GetStorageUri_InputTable = table;
            this.GetStorageUri_Output = base.GetStorageUri(table);

            return this.GetStorageUri_Output;
        }

        public CloudTable? CreateIfNotExists_InputTable;
        new public Task CreateIfNotExists(CloudTable table)
        {
            this.CreateIfNotExists_InputTable = table;

            return base.CreateIfNotExists(table);
        }
    }
}