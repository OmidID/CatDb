// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Cli.Api;
using CatDb.Cli.Commands;
using CatDb.Cli.Output;
using CatDb.Cli.Repl;
using CatDb.Cli.Session;
using Microsoft.Extensions.DependencyInjection;

namespace CatDb.Cli;

/// <summary>Composition root for the whole process: API clients (typed <see cref="HttpClient"/>s via
/// <see cref="IHttpClientFactory"/>), session/credential state, every <see cref="ICliCommand"/>, and
/// the REPL. Kept as one place so Program.cs stays a thin dispatcher.</summary>
public static class ServiceRegistration
{
    public static IServiceCollection AddCatDbCli(this IServiceCollection services)
    {
        services.AddSingleton<CliSession>();
        services.AddSingleton<CredentialResolver>();
        services.AddSingleton<IOutputWriter, ConsoleOutputWriter>();

        services.AddHttpClient<IAuthClient, AuthClient>();
        services.AddHttpClient<IAdminClient, AdminClient>();
        services.AddHttpClient<ITablesClient, TablesClient>();
        services.AddHttpClient<IDataClient, DataClient>();

        services.AddSingleton<ICliCommand, LoginCommand>();
        services.AddSingleton<ICliCommand, LogoutCommand>();
        services.AddSingleton<ICliCommand, WhoAmICommand>();
        services.AddSingleton<ICliCommand, UseCommand>();

        services.AddSingleton<ICliCommand, DbListCommand>();
        services.AddSingleton<ICliCommand, DbCreateCommand>();
        services.AddSingleton<ICliCommand, DbDeleteCommand>();
        services.AddSingleton<ICliCommand, UserListCommand>();
        services.AddSingleton<ICliCommand, UserUpsertCommand>();
        services.AddSingleton<ICliCommand, UserDeleteCommand>();

        services.AddSingleton<ICliCommand, TableListCommand>();
        services.AddSingleton<ICliCommand, TableCreateCommand>();
        services.AddSingleton<ICliCommand, TableDescribeCommand>();
        services.AddSingleton<ICliCommand, TableDeleteCommand>();
        services.AddSingleton<ICliCommand, IndexListCommand>();
        services.AddSingleton<ICliCommand, IndexCreateCommand>();
        services.AddSingleton<ICliCommand, IndexDropCommand>();
        services.AddSingleton<ICliCommand, IndexRebuildCommand>();

        services.AddSingleton<ICliCommand, RowBrowseCommand>();
        services.AddSingleton<ICliCommand, RowQueryCommand>();
        services.AddSingleton<ICliCommand, RowInsertCommand>();
        services.AddSingleton<ICliCommand, RowPutCommand>();
        services.AddSingleton<ICliCommand, RowDeleteCommand>();

        services.AddSingleton<ICliCommand, ClearCommand>();
        services.AddSingleton<ICliCommand, ExitCommand>();

        services.AddSingleton<CommandRegistry>();
        services.AddSingleton(sp => sp.GetRequiredService<CommandRegistry>().Build());
        services.AddSingleton<ReplEngine>();

        return services;
    }
}
