using System;
using System.Threading.Tasks;
using ThoughtHaven.Azure.Storage.Test.Fakes;
using Xunit;

namespace ThoughtHaven.Azure.Storage.Test.Table
{
    public class TableExistenceTesterTests
    {
        public class GetStorageUriMethod
        {
            public class TableOverload
            {
                [Fact]
                public void NullTable_Throws()
                {
                    Assert.Throws<ArgumentNullException>("table", () =>
                    {
                        Tester().GetStorageUri(table: null!);
                    });
                }

                [Fact]
                public void WhenCalled_ReturnsStorageUri()
                {
                    var table = Table(nameof(WhenCalled_ReturnsStorageUri));

                    var result = Tester().GetStorageUri(table);

                    Assert.Equal(table.StorageUri, result);
                }
            }
        }

        public class CreateIfNotExistsMethod
        {
            public class TableOverload
            {
                [Fact]
                public async Task NullTable_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("table", async () =>
                    {
                        await Tester().CreateIfNotExists(table: null!);
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsCreateIfNotExistsAsyncOnTable()
                {
                    var tester = Tester();
                    var table = Table(nameof(WhenCalled_CallsCreateIfNotExistsAsyncOnTable));

                    await tester.CreateIfNotExists(table);

                    Assert.Equal(tester.Options,
                        table.CreateIfNotExistsAsync_InputRequestOptions);
                    Assert.Null(table.CreateIfNotExistsAsync_InputOperationContext);
                }
            }
        }

        private static FakeTableExistenceTester Tester() => new FakeTableExistenceTester();
        private static FakeCloudTable Table(string tableName) => new FakeCloudTable(tableName);
    }
}