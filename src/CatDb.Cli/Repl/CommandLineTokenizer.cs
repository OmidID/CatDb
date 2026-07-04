// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text;

namespace CatDb.Cli.Repl;

/// <summary>Splits one REPL line into argv tokens, shell-style: whitespace-separated, with
/// single/double-quoted spans kept as one token (so <c>--value "a b"</c> or JSON like
/// <c>--value '{"a":1}'</c> survive intact).</summary>
public static class CommandLineTokenizer
{
    public static string[] Tokenize(string line)
    {
        var tokens = new List<string>();
        var current = new StringBuilder();
        var inToken = false;
        char quote = '\0';

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];

            if (quote != '\0')
            {
                if (c == quote) { quote = '\0'; }
                else current.Append(c);
                continue;
            }

            if (c is '"' or '\'')
            {
                quote = c;
                inToken = true;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                if (inToken) { tokens.Add(current.ToString()); current.Clear(); inToken = false; }
                continue;
            }

            current.Append(c);
            inToken = true;
        }

        if (inToken)
            tokens.Add(current.ToString());

        return tokens.ToArray();
    }
}
