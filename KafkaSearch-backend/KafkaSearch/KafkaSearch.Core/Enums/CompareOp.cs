namespace KafkaSearch.Core.Enums;

public enum CompareOp
{
    Equal, NotEqual,
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
    Contains, StartsWith, EndsWith,
    Exists
}
