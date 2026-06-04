// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database.Indexing;

/// <summary>
/// Minimum / maximum sentinel values per orderable type. Used to build exact composite-key bounds
/// so index range and prefix scans become O(log N) WTree seeks (the engine's Forward/Backward stop
/// at the bound natively) instead of scanning from an end and skipping rows.
///
/// <para>A scalar's range of keys for "field == v" is [(v, MIN_trailing…), (v, MAX_trailing…)];
/// "field &gt; v" starts strictly past (v, MAX_trailing…), etc. When every trailing slot type has a
/// sentinel the whole bound is constructed and seeked; otherwise the caller falls back to filtering.</para>
///
/// <para><c>string</c> has a minimum ("") but no maximum, so a string trailing slot disables the
/// upper-bound seek (the caller tail-filters those cases only).</para>
/// </summary>
internal static class Sentinels
{
    private static readonly Dictionary<Type, (object Min, object? Max)> Map = new()
    {
        [typeof(bool)] = (false, true),
        [typeof(byte)] = (byte.MinValue, byte.MaxValue),
        [typeof(sbyte)] = (sbyte.MinValue, sbyte.MaxValue),
        [typeof(char)] = (char.MinValue, char.MaxValue),
        [typeof(short)] = (short.MinValue, short.MaxValue),
        [typeof(ushort)] = (ushort.MinValue, ushort.MaxValue),
        [typeof(int)] = (int.MinValue, int.MaxValue),
        [typeof(uint)] = (uint.MinValue, uint.MaxValue),
        [typeof(long)] = (long.MinValue, long.MaxValue),
        [typeof(ulong)] = (ulong.MinValue, ulong.MaxValue),
        [typeof(float)] = (float.MinValue, float.MaxValue),
        [typeof(double)] = (double.MinValue, double.MaxValue),
        [typeof(decimal)] = (decimal.MinValue, decimal.MaxValue),
        [typeof(DateTime)] = (DateTime.MinValue, DateTime.MaxValue),
        [typeof(TimeSpan)] = (TimeSpan.MinValue, TimeSpan.MaxValue),
        // string: ordinal-minimum is "", but there is no maximum string.
        [typeof(string)] = ("", null),
    };

    /// <summary>
    /// Gets the sentinel value (min or max) for <paramref name="type"/>. Returns false when no
    /// suitable sentinel exists (e.g. the maximum of a string), which forces the caller to fall back
    /// to a filtered scan for that bound.
    /// </summary>
    public static bool TryGet(Type type, bool max, out object value)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (Map.TryGetValue(t, out var pair))
        {
            var v = max ? pair.Max : pair.Min;
            if (v is not null) { value = v; return true; }
        }
        value = null!;
        return false;
    }
}
