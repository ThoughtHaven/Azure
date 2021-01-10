using System;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace ThoughtHaven.Azure.Cosmos.Table
{
    public class TableResultExtensionsTests
    {
        public class IsSuccessStatusCodeMethod
        {
            public class ResultOverload
            {
                [Fact]
                public void NullResult_Throws()
                {
                    Assert.Throws<ArgumentNullException>("result", () =>
                    {
                        ((TableResult)null!).IsSuccessStatusCode();
                    });
                }

                [Fact]
                public void StatusCode200_ReturnsTrue()
                {
                    var result = new TableResult() { HttpStatusCode = 200 };

                    Assert.True(result.IsSuccessStatusCode());
                }

                [Fact]
                public void StatusCode299_ReturnsTrue()
                {
                    var result = new TableResult() { HttpStatusCode = 299 };

                    Assert.True(result.IsSuccessStatusCode());
                }

                [Fact]
                public void StatusCode199_ReturnsFalse()
                {
                    var result = new TableResult() { HttpStatusCode = 199 };

                    Assert.False(result.IsSuccessStatusCode());
                }

                [Fact]
                public void StatusCode300_ReturnsFalse()
                {
                    var result = new TableResult() { HttpStatusCode = 300 };

                    Assert.False(result.IsSuccessStatusCode());
                }
            }
        }

        public class EnsureSuccessStatusCodeMethod
        {
            public class ResultOverload
            {
                [Fact]
                public void NullResult_Throws()
                {
                    Assert.Throws<ArgumentNullException>("result", () =>
                    {
                        ((TableResult)null!).EnsureSuccessStatusCode();
                    });
                }

                [Fact]
                public void StatusCode200_DoesNotThrow()
                {
                    var result = new TableResult() { HttpStatusCode = 200 };

                    result.EnsureSuccessStatusCode();
                }

                [Fact]
                public void StatusCode299_DoesNotThrow()
                {
                    var result = new TableResult() { HttpStatusCode = 299 };

                    result.EnsureSuccessStatusCode();
                }

                [Fact]
                public void StatusCode199_Throws()
                {
                    var result = new TableResult() { HttpStatusCode = 199 };

                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        result.EnsureSuccessStatusCode();
                    });
                }

                [Fact]
                public void StatusCode300_Throws()
                {
                    var result = new TableResult() { HttpStatusCode = 300 };

                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        result.EnsureSuccessStatusCode();
                    });
                }

                [Fact]
                public void WhenThrows_HasErrorMessage()
                {
                    var result = new TableResult() { HttpStatusCode = 199 };

                    var exception = Assert.Throws<InvalidOperationException>(() =>
                    {
                        result.EnsureSuccessStatusCode();
                    });

                    Assert.Equal("TableResult has a failed status code: 199.",
                        exception.Message);
                }
            }
        }
    }
}