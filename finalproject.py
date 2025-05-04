import os
import yaml
import csv
import arcpy
import logging
from etl.GSheetsEtl import GSheetsEtl

# Logging setup
logging.basicConfig(
    filename=r"C:\Users\madch\Documents\ArcGIS\Projects\Final\wnv.log",
    filemode="w",
    level=logging.DEBUG
)

# Configuration
config_dict = {
    "proj_dir": r"C:\Users\madch\Documents\ArcGIS\Projects\Final\\",
    "gdb_path": r"C:\Users\madch\Documents\ArcGIS\Projects\APPS_LAB_1\APPS_LAB_1.gdb"
}

def load_config():
    with open('config/wnvoutbreak.yaml', 'r') as file:
        return yaml.safe_load(file)

def buffer_layer(input_layer, output_layer, distance):
    try:
        arcpy.Buffer_analysis(input_layer, output_layer, f"{distance} Feet", "FULL", "ROUND", "ALL")
        print(f"✅ Buffer created: {output_layer}")
    except Exception as e:
        logging.error(f"[buffer_layer] Error: {e}")
        print(f"❌ Buffer failed: {e}")

def erase_analysis(input_layer, erase_layer, output_layer):
    try:
        arcpy.Erase_analysis(input_layer, erase_layer, output_layer)
        print(f"✅ Erase output: {output_layer}")
    except Exception as e:
        logging.error(f"[erase_analysis] Error: {e}")
        print(f"❌ Erase failed: {e}")

def spatial_join(target, join_layer, output_layer):
    try:
        arcpy.SpatialJoin_analysis(target, join_layer, output_layer)
        print(f"✅ Spatial Join completed: {output_layer}")
    except Exception as e:
        logging.error(f"[spatial_join] Error: {e}")
        print(f"❌ Spatial Join failed: {e}")

def count_addresses(joined_layer):
    try:
        count = 0
        with arcpy.da.SearchCursor(joined_layer, ["Join_Count"]) as cursor:
            for row in cursor:
                if row[0] > 0:
                    count += 1
        print(f"📌 {count} addresses to notify.")
        return count
    except Exception as e:
        logging.error(f"[count_addresses] Error: {e}")
        print(f"❌ Count failed: {e}")
        return 0

def export_notified_addresses(input_fc, output_fc):
    try:
        arcpy.MakeFeatureLayer_management(input_fc, "temp_layer", "Join_Count > 0")
        arcpy.CopyFeatures_management("temp_layer", output_fc)
        print(f"✅ Notified addresses exported: {output_fc}")
    except Exception as e:
        logging.error(f"[export_notified_addresses] Error: {e}")
        print(f"❌ Export failed: {e}")

def export_addresses_to_csv(input_fc, output_csv_path):
    try:
        fields = ['FULLADDR']
        print(f"🔍 Checking feature class: {input_fc}")
        if not arcpy.Exists(input_fc):
            print(f"❌ Feature class does not exist: {input_fc}")
            return

        count = int(arcpy.GetCount_management(input_fc)[0])
        print(f"🔍 Record count: {count}")
        if count == 0:
            print("⚠️ No records to export.")
            return

        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['FullAddress'])
            with arcpy.da.SearchCursor(input_fc, fields) as cursor:
                for row in cursor:
                    writer.writerow(row)
        print(f"✅ CSV exported: {output_csv_path}")
        logging.info(f"Exported CSV to: {output_csv_path}")

    except Exception as e:
        logging.error(f"[export_addresses_to_csv] Error: {e}")
        print(f"❌ CSV export failed: {e}")

def apply_renderer_and_definition(aprx):
    try:
        map_obj = aprx.listMaps()[0]
        final_layer = map_obj.listLayers("Risk_Intersect_Final")[0]
        final_layer.transparency = 50

        arcpy.analysis.SpatialJoin(
            target_features="Addresses",
            join_features="Risk_Intersect_Final",
            out_feature_class="Target_Addresses",
            join_type="KEEP_COMMON"
        )

        target_layer = map_obj.listLayers("Target_Addresses")[0]
        target_layer.definitionQuery = "Join_Count = 1"
        print("✅ Renderer and definition query applied.")
    except Exception as e:
        logging.error(f"[apply_renderer_and_definition] Error: {e}")
        print(f"❌ Renderer or query failed: {e}")

def exportMap(aprx):
    try:
        subtitle = input("Enter a subtitle for the map: ")
        layout = aprx.listLayouts()[0]
        for el in layout.listElements("TEXT_ELEMENT"):
            if el.name == "Title":
                el.text = f"West Nile Virus Model - {subtitle}"
        pdf_path = os.path.join(config_dict["proj_dir"], f"WNV_Map_{subtitle}.pdf")
        layout.exportToPDF(pdf_path)
        print(f"✅ PDF map exported: {pdf_path}")
        logging.info(f"Map exported to: {pdf_path}")
    except Exception as e:
        logging.error(f"[exportMap] Error: {e}")
        print(f"❌ Map export failed: {e}")

def main():
    print("🚀 Starting West Nile Virus Simulation...")
    config = load_config()

    aprx_path = os.path.join(config_dict["proj_dir"], "final.aprx")
    aprx = arcpy.mp.ArcGISProject(aprx_path)

    arcpy.env.workspace = config_dict["gdb_path"]
    arcpy.env.overwriteOutput = True
    print(f"📁 Workspace set to: {arcpy.env.workspace}")

    # ETL
    etl = GSheetsEtl(config)
    etl.process()

    # Spatial Analysis
    buffer_layer("Avoid_Points", "Avoid_Points_Buffer", 1500)
    erase_analysis("Risk_Intersect", "Avoid_Points_Buffer", "Risk_Intersect_Final")
    spatial_join("Addresses", "Risk_Intersect_Final", "Addresses_To_Notify")
    count_addresses("Addresses_To_Notify")
    export_notified_addresses("Addresses_To_Notify", "Addresses_To_Notify_Clean")

    # CSV Export
    fc_path = os.path.join(config_dict["gdb_path"], "Addresses_To_Notify_Clean")
    csv_path = os.path.join(config_dict["proj_dir"], "Target_Addresses.csv")
    export_addresses_to_csv(fc_path, csv_path)

    # Map Styling and PDF Export
    apply_renderer_and_definition(aprx)
    exportMap(aprx)

if __name__ == "__main__":
    main()
