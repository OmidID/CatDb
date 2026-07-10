# CatDb.Server — HTTP admin/data API (5100) + raw TCP data protocol (7182)
# Build from repo root: docker build -t catdb-server -f Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props Directory.Packages.props ./
COPY src/CatDb/CatDb.csproj src/CatDb/
COPY src/CatDb.Server/CatDb.Server.csproj src/CatDb.Server/
RUN dotnet restore src/CatDb.Server/CatDb.Server.csproj

COPY src/CatDb/ src/CatDb/
COPY src/CatDb.Server/ src/CatDb.Server/
RUN dotnet publish src/CatDb.Server/CatDb.Server.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin catdb \
    && mkdir -p /data \
    && chown -R catdb:catdb /app /data
USER catdb

COPY --from=build --chown=catdb:catdb /app ./

ENV ASPNETCORE_URLS=http://+:5100 \
    CatDb__Directory=/data \
    CatDb__Port=7182

VOLUME ["/data"]
EXPOSE 5100 7182

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:5100/health || exit 1

# --urls beats appsettings.json's "Urls": "http://localhost:5100" (a loopback-only dev default) —
# command-line config wins over both appsettings.json and env vars, so this is the only reliable
# way to make Kestrel bind 0.0.0.0 instead of silently staying on 127.0.0.1 inside the container
# (ASPNETCORE_URLS alone was NOT enough to override the appsettings.json value — verified empirically).
ENTRYPOINT ["dotnet", "CatDb.Server.dll", "--urls", "http://+:5100"]
