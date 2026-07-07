// Repro for reported: NonUnique index + Guid primary key -> NRE in OrderedSet.FindIndexes
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

public class GuidPkNonUniqueReproTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Entity
    {
        public Guid AppId { get; set; }
        public string City { get; set; } = "";
        public int Age { get; set; }
    }

    [Fact]
    public void GuidPk_NonUnique_StringField()
    {
        var t = _engine.OpenXTable<Guid, Entity>("g_str");
        t.CreateIndex("City", e => e.City, IndexType.NonUnique);
        for (int i = 0; i < 50; i++)
            t.Replace(Guid.NewGuid(), new Entity { AppId = Guid.NewGuid(), City = i % 2 == 0 ? "NYC" : "LA", Age = i });
        _engine.Commit();

        var res = t.Query(x => x.City).Equal("NYC").ToList();
        res.Should().HaveCount(25);
    }

    [Fact]
    public void GuidPk_NonUnique_GuidField()
    {
        var t = _engine.OpenXTable<Guid, Entity>("g_guid");
        t.CreateIndex("AppId", e => e.AppId, IndexType.NonUnique);
        var app = Guid.NewGuid();
        for (int i = 0; i < 20; i++)
            t.Replace(Guid.NewGuid(), new Entity { AppId = i < 10 ? app : Guid.NewGuid(), City = "X", Age = i });
        _engine.Commit();

        var res = t.Query(x => x.AppId).Equal(app).ToList();
        res.Should().HaveCount(10);
    }
}
