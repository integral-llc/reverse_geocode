using System.Globalization;
using System.IO.Compression;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using Supercluster.KDTree;

namespace reverse_geocode;

public class GeocodeData
{
    const string GEOCODE_URL = "http://download.geonames.org/export/dump/cities1000.zip";
    private const string GEOCODE_FILENAME = "cities1000.txt";
    private const string STATE_CODE_URL = "http://download.geonames.org/export/dump/admin1CodesASCII.txt";
    private const string COUNTY_CODE_URL = "https://download.geonames.org/export/dump/admin2Codes.txt";

    private Dictionary<string, string> _countries;
    private readonly List<Location> _locations;
    private readonly KDTree<double, int> _kdtree;

    public GeocodeData(int minPopulation = 0,
        string geocodeFilename = "geocode.gz",
        string countryFilename = "countries.csv")
    {
        LoadCountries(countryFilename);
        _locations = Extract(geocodeFilename, minPopulation).Result;
        var coordinates = _locations.Select(l => new[] { l.Latitude, l.Longitude }).ToArray();

        _kdtree = new KDTree<double, int>(2, coordinates, 
            coordinates.Select((val, index) => index).ToArray(), 
            L2NormSquaredDouble,
            double.MinValue,
            double.MaxValue);
        return;

        double L2NormSquaredDouble(double[] x, double[] y)
        {
            return x.Select((t, i) => (t - y[i]) * (t - y[i])).Sum();
        }
    }

    public List<Location> Query(double lat, double lon)
    {
        var searchResult = _kdtree.RadialSearch(new[] { lat, lon }, 1);
        var results = searchResult.Select(tuple => _locations[tuple.Item2]).ToList();
        foreach (var result in results)
        {
            result.Country = _countries.GetValueOrDefault(result.CountryCode, "");
        }

        return results;
    }

    private async Task<List<Location>> Extract(string localFileName, int minPopulation)
    {
        List<Location> locations;
        if (File.Exists(localFileName))
        {
            locations = UnZipToJson<List<Location>>(localFileName);
        }
        else
        {
            var (geoReader, stateMap, countyMap) = await DownloadGeocode();
            locations = new List<Location>();
            while (geoReader.Read())
            {
                string countryCode = geoReader.GetField(8);
                string city = geoReader.GetField(1);
                double latitude = geoReader.GetField<double>(4);
                double longitude = geoReader.GetField<double>(5);
                int population = geoReader.GetField<int>(14);
                string stateCode = geoReader.GetField(10);
                string countyCode = geoReader.GetField(11);

                string state = stateMap.GetValueOrDefault(stateCode, "");
                string county = countyMap.GetValueOrDefault(countyCode, "");
                locations.Add(new Location
                {
                    CountryCode = countryCode,
                    City = city,
                    Latitude = latitude,
                    Longitude = longitude,
                    Population = population,
                    State = state,
                    County = county
                });
            }
        }

        if (minPopulation > 0)
        {
            locations = locations.Where(l => l.Population >= minPopulation).ToList();
        }

        return locations;
    }

    private static Dictionary<string, string> GetCodeMap(CsvReader csv)
    {
        Dictionary<string, string> codeMap = new();
        while (csv.Read())
        {
            string code = csv.GetField(0);
            string name = csv.GetField(1);
            codeMap[code] = name;
        }

        return codeMap;
    }

    private static async Task<(CsvReader geocodeReader,
        Dictionary<string, string> stateMap,
        Dictionary<string, string> countyMap)> DownloadGeocode()
    {
        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = "\t", HasHeaderRecord = false
        };

        var geocodeData = await DownloadToByteArray(GEOCODE_URL);
        await using ZLibStream zipStream = new ZLibStream(new MemoryStream(geocodeData), CompressionMode.Decompress);
        var unzippedData = await new StreamReader(zipStream).ReadToEndAsync();
        CsvReader geocodeReader = GetCsvReader(unzippedData);

        CsvReader stateReader = GetCsvReader(await DownloadToString(STATE_CODE_URL));
        CsvReader countyReader = GetCsvReader(await DownloadToString(COUNTY_CODE_URL));

        Dictionary<string, string> stateMap = GetCodeMap(stateReader);
        Dictionary<string, string> countyMap = GetCodeMap(countyReader);
        return (geocodeReader, stateMap, countyMap);

        CsvReader GetCsvReader(string data)
        {
            return new CsvReader(new StringReader(data), csvConfig);
        }
    }

    private static async Task<string> DownloadToString(string url)
    {
        using HttpClient client = new();
        return await client.GetStringAsync(url);
    }

    private static async Task<byte[]> DownloadToByteArray(string url)
    {
        using HttpClient client = new();
        return await client.GetByteArrayAsync(url);
    }

    private static T UnZipToJson<T>(string fileName)
    {
        using GZipStream zipStream = new(File.Open(fileName, FileMode.Open), CompressionMode.Decompress);
        using StreamReader reader = new(zipStream);
        string json = reader.ReadToEnd();
        return JsonSerializer.Deserialize<T>(json);
    }

    private void LoadCountries(string countryFilename)
    {
        using CsvReader csv = new(new StreamReader(countryFilename), CultureInfo.InvariantCulture);
        _countries = new Dictionary<string, string>();
        while (csv.Read())
        {
            string code = csv.GetField(0);
            string name = csv.GetField(1);
            _countries[code] = name;
        }
    }
}

public class Location
{
    public string CountryCode { get; set; }
    public string Country { get; set; }
    public string City { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    public int Population { get; set; }
    public string State { get; set; }
    public string County { get; set; }
}