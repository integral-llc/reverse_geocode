// See https://aka.ms/new-console-template for more information

using reverse_geocode;

Console.WriteLine("Hello, World!");

var geocodeData = new GeocodeData();

var result = geocodeData.Query(37.7749, -122.4194);

foreach (var location in result)
{
    Console.WriteLine(location.City, location.Country);
}
