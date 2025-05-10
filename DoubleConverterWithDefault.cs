using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

public class DoubleConverterWithDefault : DoubleConverter
{
    public override object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
    {
        if (string.IsNullOrWhiteSpace(text))  
        {
            return null; 
        }
        return base.ConvertFromString(text, row, memberMapData);
    }
}
