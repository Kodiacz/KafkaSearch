using System.Text.Json.Serialization;

namespace KafkaSearch.Core.Filtering;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(And), "and")]
[JsonDerivedType(typeof(Or), "or")]
[JsonDerivedType(typeof(Any), "any")]
[JsonDerivedType(typeof(Compare), "compare")]
public abstract record FilterNode;
