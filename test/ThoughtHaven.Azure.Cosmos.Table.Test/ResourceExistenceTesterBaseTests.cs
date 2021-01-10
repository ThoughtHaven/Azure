using System;
using System.Threading.Tasks;
using ThoughtHaven.Azure.Cosmos.Table.Fakes;
using Xunit;

namespace ThoughtHaven.Azure.Cosmos.Table
{
    public class ResourceExistenceTesterBaseTests
    {
        public class EnsureExistsMethod
        {
            public class ResourceOverload
            {
                [Fact]
                public async Task NullResource_Throws()
                {
                    await Assert.ThrowsAsync<ArgumentNullException>("resource", async () =>
                    {
                        await Tester(nameof(NullResource_Throws))
                            .EnsureExists(resource: null!);
                    });
                }

                [Fact]
                public async Task WhenCalled_CallsGetStorageUriOnAbstract()
                {
                    var tester = Tester(nameof(WhenCalled_CallsGetStorageUriOnAbstract));
                    var resource = new object();

                    await tester.EnsureExists(resource);

                    Assert.Equal(resource, tester.GetStorageUri_InputResource);
                }

                [Fact]
                public async Task IfNotKnownToExist_CallsCreateIfNotExistsOnAbstract()
                {
                    var tester = Tester(nameof(IfNotKnownToExist_CallsCreateIfNotExistsOnAbstract));
                    var resource = new object();

                    await tester.EnsureExists(resource);

                    Assert.Equal(resource, tester.CreateIfNotExists_InputResource);
                }

                [Fact]
                public async Task IfKnownToExist_DoesNotCallCreateIfNotExistsOnAbstract()
                {
                    var resource = new object();
                    var tester = Tester(nameof(IfKnownToExist_DoesNotCallCreateIfNotExistsOnAbstract));
                    tester.KnownToExist.Add(tester.TestStorageUri);

                    await tester.EnsureExists(resource);

                    Assert.Null(tester.CreateIfNotExists_InputResource);
                }
            }
        }

        private static FakeResourceExistenceTesterBase Tester(string testName) =>
            new FakeResourceExistenceTesterBase(new Uri($"https://example.com/{testName}"));
    }
}