// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Tests.Data;

/// <summary>Shared test models used across multiple test classes.</summary>
public struct TestKey
{
    public int Id { get; set; }
    public string Name { get; set; }
}

public class Tick
{
    public string Symbol { get; set; } = "";
    public DateTime Timestamp { get; set; }
    public double Bid { get; set; }
    public double Ask { get; set; }
    public int BidSize { get; set; }
    public int AskSize { get; set; }
    public string Provider { get; set; } = "";

    public Tick() { }

    public Tick(string symbol, DateTime time, double bid, double ask, int bidSize, int askSize, string provider)
    {
        Symbol = symbol;
        Timestamp = time;
        Bid = bid;
        Ask = ask;
        BidSize = bidSize;
        AskSize = askSize;
        Provider = provider;
    }

    public override bool Equals(object? obj) =>
        obj is Tick t && Symbol == t.Symbol && Timestamp == t.Timestamp
            && Bid == t.Bid && Ask == t.Ask && BidSize == t.BidSize
            && AskSize == t.AskSize && Provider == t.Provider;

    public override int GetHashCode() =>
        HashCode.Combine(Symbol, Timestamp, Bid, Ask, BidSize, AskSize, Provider);
}

public struct PriceKey
{
    public string Symbol { get; set; }
    public DateTime Date { get; set; }
}
