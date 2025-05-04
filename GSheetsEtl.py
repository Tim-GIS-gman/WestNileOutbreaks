import requests
import csv
import io
import arcpy

class GSheetsEtl:
    """
    A class to perform ETL (Extract, Transform, Load) operations on data from a Google Sheet.
    Designed specifically for geospatial workflows using ArcPy and ArcGIS.

    Attributes:
        config (dict): Configuration dictionary containing keys like 'remote_url', 'proj_dir', and 'geocoder_prefix_url'.

    Methods:
        extract(): Downloads raw CSV data from the Google Sheet URL.
        transform(data): Parses the CSV data into a list of dictionaries.
        load(data): Loads the transformed data into a geodatabase as geocoded point features.
        process(): Convenience method to run the full ETL pipeline in sequence.
    """

    def __init__(self, config):
        """
        Initializes the GSheetsEtl class with configuration settings.

        Args:
            config (dict): Configuration with URLs and file paths.
        """
        self.config = config

    def extract(self):
        """
        Extracts CSV data from the remote Google Sheets URL.

        Returns:
            str or None: The raw CSV data as a string if successful, otherwise None.
        """
        print("Extracting data from Google Sheets...")
        url = self.config.get("remote_url")
        print(f"Using URL: {url}")
        response = requests.get(url)

        print(f"Response code: {response.status_code}")
        if response.status_code == 200:
            data = response.text
            print("Raw CSV Data Preview:")
            print(data[:300])  # Show the first 300 characters
            return data
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None

    def transform(self, data):
        """
        Transforms raw CSV text into structured data.

        Args:
            data (str): Raw CSV text data.

        Returns:
            list[dict]: A list of records, each as a dictionary.
        """
        print("Transforming data...")
        if not data:
            print("No data received for transformation.")
            return []

        try:
            reader = csv.DictReader(io.StringIO(data))
            records = list(reader)
            print(f"Transformed {len(records)} rows.")
            for i, row in enumerate(records[:5]):
                print(f"Row {i + 1}: {row}")
            return records
        except Exception as e:
            print("Error during transformation:", e)
            return []

    def load(self, data):
        """
        Loads transformed data into a geodatabase feature class using ArcPy.

        Args:
            data (list[dict]): A list of transformed records.

        Returns:
            None
        """
        print("Loading data into feature class 'Avoid_Points'...")

        if not data:
            print("No data to load.")
            return

        fc_name = "Avoid_Points"
        gdb_path = f"{self.config['proj_dir']}APPS_LAB_1.gdb"
        output_fc = f"{gdb_path}\\{fc_name}"

        if arcpy.Exists(output_fc):
            arcpy.Delete_management(output_fc)

        spatial_ref = arcpy.SpatialReference(4326)  # WGS 84
        arcpy.CreateFeatureclass_management(gdb_path, fc_name, "POINT", spatial_reference=spatial_ref)

        arcpy.AddField_management(output_fc, "FullAddress", "TEXT")

        with arcpy.da.InsertCursor(output_fc, ["SHAPE@XY", "FullAddress"]) as cursor:
            for row in data:
                full_address = f"{row['Address']}, {row['City']}, {row['State']} {row['ZIP']}"
                geocode_url = f"{self.config['geocoder_prefix_url']}World/GeocodeServer/findAddressCandidates"
                params = {
                    'f': 'json',
                    'SingleLine': full_address,
                    'outFields': '*'
                }
                response = requests.get(geocode_url, params=params)
                candidates = response.json().get('candidates')
                if candidates:
                    location = candidates[0]['location']
                    cursor.insertRow(((location['x'], location['y']), full_address))
                    print(f"Geocoded: {full_address}")
                else:
                    print(f"Failed to geocode: {full_address}")

    def process(self):
        """
        Runs the complete ETL pipeline: extract, transform, and load.

        Returns:
            None
        """
        raw_data = self.extract()
        if raw_data:
            transformed = self.transform(raw_data)
            self.load(transformed)
