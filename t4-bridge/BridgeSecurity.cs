using System.Security.Cryptography;
using System.Text;

namespace CryptoAgent.T4Bridge;

public static class BridgeSecurity
{
    public const string TokenHeaderName = "X-Crypto-Agent-Bridge-Token";

    public static bool TokenMatches(string supplied, string expected)
    {
        var suppliedHash = SHA256.HashData(Encoding.UTF8.GetBytes(supplied));
        var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expected));
        return CryptographicOperations.FixedTimeEquals(suppliedHash, expectedHash);
    }
}
