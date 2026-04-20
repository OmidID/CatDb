using CatDb.Data;
using FluentAssertions;

namespace CatDb.Tests.Data;

/// <summary>
/// Tests for SlotsBuilder — the dynamic generic type emitter used by the anonymous data type system.
/// The CodeDom/Mono path was removed; this validates the ILGenerator (Emit) path still works.
/// </summary>
public class SlotsBuilderTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void BuildType_Prebaked_ReturnsGenericType(int slotCount)
    {
        var types = Enumerable.Range(0, slotCount).Select(_ => typeof(int)).ToArray();
        var builtType = SlotsBuilder.BuildType(types);

        builtType.Should().NotBeNull();
        builtType.IsGenericType.Should().BeTrue();
        builtType.GetGenericArguments().Length.Should().Be(slotCount);
    }

    [Fact]
    public void BuildType_MaxPrebakedSlots_Returns16Slot()
    {
        var types = Enumerable.Range(0, 16).Select(_ => typeof(string)).ToArray();
        var builtType = SlotsBuilder.BuildType(types);
        builtType.Should().NotBeNull();
        builtType.GetGenericArguments().Should().HaveCount(16);
    }

    [Fact]
    public void BuildType_EmptyTypes_Throws()
    {
        var act = () => SlotsBuilder.BuildType(Array.Empty<Type>());
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void BuildType_MixedTypes_Works()
    {
        var types = new[] { typeof(int), typeof(string), typeof(DateTime), typeof(double) };
        var builtType = SlotsBuilder.BuildType(types);
        builtType.Should().NotBeNull();
        var genericArgs = builtType.GetGenericArguments();
        genericArgs[0].Should().Be(typeof(int));
        genericArgs[1].Should().Be(typeof(string));
        genericArgs[2].Should().Be(typeof(DateTime));
        genericArgs[3].Should().Be(typeof(double));
    }

    [Fact]
    public void BuildType_IsCached_ReturnsSameTypeOnSecondCall()
    {
        var types = new[] { typeof(long), typeof(string) };
        var type1 = SlotsBuilder.BuildType(types);
        var type2 = SlotsBuilder.BuildType(types);
        type1.Should().BeSameAs(type2);
    }

    [Fact]
    public void BuildType_CanInstantiateResult()
    {
        var types = new[] { typeof(int), typeof(string) };
        var builtType = SlotsBuilder.BuildType(types);
        var instance = Activator.CreateInstance(builtType);
        instance.Should().NotBeNull();
    }
}
