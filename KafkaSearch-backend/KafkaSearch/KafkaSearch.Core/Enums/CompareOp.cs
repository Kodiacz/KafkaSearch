namespace KafkaSearch.Core.Enums;

public enum CompareOp
{
    Eq, NotEq,
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
    Contains, StartsWith, EndsWith,
    Exists
}
