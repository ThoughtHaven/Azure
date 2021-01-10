using System;

namespace Microsoft.Azure.Cosmos.Table
{
    public static class TableResultExtensions
    {
        public static bool IsSuccessStatusCode(this TableResult result)
        {
            if (result is null) { throw new ArgumentNullException(nameof(result)); }

            return result.HttpStatusCode >= 200 && result.HttpStatusCode < 300;
        }

        public static void EnsureSuccessStatusCode(this TableResult result)
        {
            if (result is null) { throw new ArgumentNullException(nameof(result)); }

            if (!result.IsSuccessStatusCode())
            {
                throw new InvalidOperationException($"{nameof(TableResult)} has a failed status code: {result.HttpStatusCode}.");
            }
        }
    }
}