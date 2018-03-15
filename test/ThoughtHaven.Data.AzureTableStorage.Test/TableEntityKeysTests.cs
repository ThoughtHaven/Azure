using System;
using Xunit;

namespace ThoughtHaven.Data
{
    public class TableEntityKeysTests
    {
        public class Constructor
        {
            public class PartitionKeyAndRowKeyOverload
            {
                [Fact]
                public void NullPartitionKey_Throws()
                {
                    Assert.Throws<ArgumentNullException>("partitionKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: null,
                            rowKey: "rk");
                    });
                }

                [Fact]
                public void EmptyPartitionKey_Throws()
                {
                    Assert.Throws<ArgumentException>("partitionKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: "",
                            rowKey: "rk");
                    });
                }

                [Fact]
                public void WhiteSpacePartitionKey_Throws()
                {
                    Assert.Throws<ArgumentException>("partitionKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: " ",
                            rowKey: "rk");
                    });
                }

                [Fact]
                public void NullRowKey_Throws()
                {
                    Assert.Throws<ArgumentNullException>("rowKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: "pk",
                            rowKey: null);
                    });
                }

                [Fact]
                public void EmptyRowKey_Throws()
                {
                    Assert.Throws<ArgumentException>("rowKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: "pk",
                            rowKey: "");
                    });
                }

                [Fact]
                public void WhiteSpaceRowKey_Throws()
                {
                    Assert.Throws<ArgumentException>("rowKey", () =>
                    {
                        new TableEntityKeys(
                            partitionKey: "pk",
                            rowKey: " ");
                    });
                }

                [Fact]
                public void WhenCalled_SetsPartitionKey()
                {
                    var keys = new TableEntityKeys("pk", "rk");

                    Assert.Equal("pk", keys.PartitionKey);
                }

                [Fact]
                public void WhenCalled_SetsRowKey()
                {
                    var keys = new TableEntityKeys("pk", "rk");

                    Assert.Equal("rk", keys.RowKey);
                }
            }
        }
    }
}