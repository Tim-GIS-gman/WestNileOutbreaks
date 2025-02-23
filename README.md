import arcpy
import os

arcpy.env.overwriteOutput = True

def buffer_and_add_to_project(layer_path, distance, output_gdb, project_path):
    layer_name = os.path.splitext(os.path.basename(layer_path))[0]
    output_layer = f"{layer_name}_buffer_{distance.replace(' ', '')}"
    output_path = os.path.join(output_gdb, output_layer)

    arcpy.analysis.Buffer(layer_path, output_path, distance, "FULL", "ROUND", "ALL")

    aprx = arcpy.mp.ArcGISProject(project_path)
    map = aprx.listMaps()[0]
    map.addDataFromPath(output_path)
    aprx.save()

    print(f"Buffer created and added to project: {output_layer}")

def main():
    # Paths to your layers
    layers = {
        "Mosquito Larval Sites": r"E:\A_Spring_2025\GIS_APPS\Week_5\Lab_1\arcgis_westnileoutbreak_shapefiles\Mosquito_Larval_Sites\Mosquito_Larval_Sites.shp",
        "Wetlands": r"E:\A_Spring_2025\GIS_APPS\Week_5\Lab_1\arcgis_westnileoutbreak_shapefiles\Wetlands\Wetlands.shp",
        "Lakes and Reservoirs": r"E:\A_Spring_2025\GIS_APPS\Week_5\Lab_1\arcgis_westnileoutbreak_shapefiles\Lakes_and_Reservoirs_-_Boulder_County\Lakes_and_Reservoirs_-_Boulder_County.shp",
        "OSMP Properties": r"E:\A_Spring_2025\GIS_APPS\Week_5\Lab_1\arcgis_westnileoutbreak_shapefiles\OSMP_Properties\OSMP_Properties.shp"
    }

    output_gdb = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb"
    project_path = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.aprx"

    for layer_name, layer_path in layers.items():
        print(f"\nBuffer Analysis for layer: {layer_name}")
        distance = input(f"Enter buffer distance in feet (1000-5000 recommended) for {layer_name}: ")
        buffer_distance = f"{distance} Feet"

        buffer_and_add_to_project(layer_path, buffer_distance, output_gdb, project_path)

if __name__ == "__main__":
    main()

import arcpy
import os

arcpy.env.overwriteOutput = True

def intersect_buffers(buffer_layers, output_gdb, project_path):
    output_name = input("Enter output name for intersected layer (no spaces): ")
    output_path = os.path.join(output_gdb, output_name)

    arcpy.analysis.Intersect(buffer_layers, output_path, "ALL")

    aprx = arcpy.mp.ArcGISProject(project_path)
    map = aprx.listMaps()[0]
    map.addDataFromPath(output_path)
    aprx.save()

    print(f"Intersect completed and added to project: {output_name}")

if __name__ == "__main__":
    buffer_layers = [
        r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb\Mosquito_Larval_Sites_buffer_1000Feet",
        r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb\Wetlands_buffer_1000Feet",
        r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb\Lakes_and_Reservoirs___Boulder_County_buffer_1000Feet",
        r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb\OSMP_Properties_buffer_1000Feet"
    ]

    output_gdb = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb"
    project_path = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.aprx"

    intersect_buffers(buffer_layers, output_gdb, project_path)

import arcpy
import os

arcpy.env.overwriteOutput = True

def spatial_join_addresses(address_layer, intersect_layer, output_gdb, project_path):
    output_name = "Addresses_SpatialJoin"
    output_path = os.path.join(output_gdb, output_name)

    # Perform Spatial Join
    arcpy.analysis.SpatialJoin(
        target_features=address_layer,
        join_features=intersect_layer,
        out_feature_class=output_path,
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT"
    )

    # Add result to project
    aprx = arcpy.mp.ArcGISProject(project_path)
    map_doc = aprx.listMaps()[0]
    map_doc.addDataFromPath(output_path)
    aprx.save()

    print(f"Spatial join completed and added to project: {output_name}")


    count = arcpy.management.GetCount(
        arcpy.management.SelectLayerByAttribute(output_path, "NEW_SELECTION", "Join_Count = 1")
    )
    print(f"Number of addresses within intersect area: {count}")

if __name__ == "__main__":
    address_layer = r"E:\A_Spring_2025\GIS_APPS\Week_5\Lab_1\arcgis_westnileoutbreak_shapefiles\Addresses\Addresses.shp"

    # Update this intersect layer path if different
    intersect_layer = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb\intersect"

    output_gdb = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.gdb"
    project_path = r"E:\ARC_Projects\WestNileOutbreak\WestNileOutbreak.aprx"

    spatial_join_addresses(address_layer, intersect_layer, output_gdb, project_path)

