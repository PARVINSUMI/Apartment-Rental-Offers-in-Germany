{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "4343ac92-0a10-4808-9261-f2840819f823",
   "metadata": {},
   "outputs": [],
   "source": [
    "import csv \n",
    "import pandas as pd\n",
    "\n",
    "def prepare_data_for_normalized_schema(csv_file):\n",
    "    pd.read_csv('german_clean_immo_data.csv')\n",
    "csv_file = ('german_clean_immo_data.csv')\n",
    "# Example usage:\n",
    "# Call the function with your CSV file name\n",
    "#prepare_data_for_normalized_schema('german_clean_immo_data.csv')\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "73a06ae4-6d21-4282-92a0-7f1f88ac785b",
   "metadata": {},
   "outputs": [],
   "source": [
    "  # Dictionaries and sets to keep track of unique records and primary keys\n",
    "locations = {}\n",
    "amenities = {}\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "f6ee890a-ec54-4672-969e-7b6252729384",
   "metadata": {},
   "outputs": [],
   "source": [
    " # Counters for generating new primary keys\n",
    "location_id_counter = 1\n",
    "property_id_counter = 1\n",
    "amenity_id_counter = 1\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "b7e1720a-eca7-4e53-b8de-b5b9cb4ad1c3",
   "metadata": {},
   "outputs": [],
   "source": [
    "  # Lists to store the SQL statements\n",
    "location_sql = []\n",
    "amenity_sql = []\n",
    "property_sql = []\n",
    "rent_sql = []\n",
    "condition_sql = []\n",
    "property_amenities_sql = []"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "id": "3d7d4900-dc66-4535-98cf-93fa2f1b019e",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Hardcoded list of amenity columns from the CSV\n",
    "amenity_cols = [\n",
    "    ('has_kitchen', 'has_kitchen'),\n",
    "    ('has_cellar', 'has_cellar'),\n",
    "    ('has_lift', 'has_lift'),\n",
    "    ('has_balcony', 'has_balcony'),\n",
    "    ('has_garden', 'has_garden')\n",
    "]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "f4b4bcbc-b07d-4442-a8ff-09ce9a885151",
   "metadata": {},
   "outputs": [],
   "source": [
    " # Pre-populate amenities lookup table to avoid multiple inserts\n",
    "for col_name, amenity_name in amenity_cols:\n",
    "    amenities[amenity_name] = amenity_id_counter\n",
    "    amenity_sql.append(f\"INSERT INTO Amenities (amenity_id, amenity_name) VALUES ({amenity_id_counter}, '{amenity_name}');\")\n",
    "    amenity_id_counter += 1\n",
    "\n",
    "with open(csv_file, mode='r', encoding='utf-8') as file:\n",
    "    reader = csv.DictReader(file)\n",
    "    for row in reader:\n",
    "        # --- Location Data ---\n",
    "        location_key = (row['geo_district'], row['region_state'])\n",
    "        if location_key not in locations:\n",
    "            locations[location_key] = location_id_counter\n",
    "            location_sql.append(\n",
    "                f\"INSERT INTO Location (location_id, geo_district, region_state) VALUES ({location_id_counter}, '{row['geo_district']}', '{row['region_state']}');\"\n",
    "            )\n",
    "            current_location_id = location_id_counter\n",
    "            location_id_counter += 1\n",
    "        else:\n",
    "            current_location_id = locations[location_key]\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "3b543117-6171-4e6d-868d-ec4b08d05fa6",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "            # --- Property Data ---\n",
    "            property_sql.append(\n",
    "                f\"INSERT INTO Property (property_id, flat_type, number_of_rooms, year_constructed, living_space, listing_date, location_id) VALUES \"\n",
    "                f\"({property_id_counter}, '{row['flat_type']}', {row['number_of_rooms']}, {row['year_constructed']}, {row['living_space']}, '{row['listing_date']}', {current_location_id});\"\n",
    "            )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "e64d284f-61cd-4f76-9bd1-28168db0d4ec",
   "metadata": {},
   "outputs": [],
   "source": [
    " # --- Rent Data ---\n",
    "    # NOTE: We use the same ID for rent_id and property_id to maintain the one-to-one relationship\n",
    "rent_sql.append(\n",
    "        f\"INSERT INTO Rent (rent_id, base_rent, service_charge, total_rent, property_id) VALUES \"\n",
    "        f\"({property_id_counter}, {row['base_rent']}, {row['service_charge']}, {row['total_rent']}, {property_id_counter});\"\n",
    "    )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "id": "7df75c9b-6068-4eda-9f53-ad03547e8b62",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "# --- Condition Data ---\n",
    "# NOTE: We use the same ID for condition_id and property_id for the one-to-one relationship\n",
    "condition_sql.append(\n",
    "    f\"INSERT INTO Condition (condition_id, condition, interior_quality, newly_constructed, property_id) VALUES \"\n",
    "    f\"({property_id_counter}, '{row['condition']}', '{row['interior_quality']}', {row['newly_constructed'].upper()}, {property_id_counter});\"\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "id": "0cb2399f-e6fe-447d-9cce-2c621bd5323d",
   "metadata": {},
   "outputs": [],
   "source": [
    " # --- Property_Amenities Data ---\n",
    "for col_name, amenity_name in amenity_cols:\n",
    "    if row[col_name].upper() == 'TRUE':\n",
    "        current_amenity_id = amenities[amenity_name]\n",
    "        property_amenities_sql.append(\n",
    "            f\"INSERT INTO Property_Amenities (property_id, amenity_id) VALUES ({property_id_counter}, {current_amenity_id});\"\n",
    "        )\n",
    "\n",
    "property_id_counter += 1"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "id": "becb462d-8a62-454e-9e1d-ae1360e581ea",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "SQL statements have been successfully exported to 'insert_data.sql'.\n"
     ]
    }
   ],
   "source": [
    "    # Write all the generated SQL statements to a file\n",
    "    output_filename = \"insert_data.sql\"\n",
    "    with open(output_filename, 'w', encoding='utf-8') as outfile:\n",
    "        outfile.write(\"-- --- AMENITIES INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(amenity_sql) + '\\n\\n')\n",
    "        outfile.write(\"-- --- LOCATION INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(location_sql) + '\\n\\n')\n",
    "        outfile.write(\"-- --- PROPERTY INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(property_sql) + '\\n\\n')\n",
    "        outfile.write(\"-- --- RENT INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(rent_sql) + '\\n\\n')\n",
    "        outfile.write(\"-- --- CONDITION INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(condition_sql) + '\\n\\n')\n",
    "        outfile.write(\"-- --- PROPERTY_AMENITIES INSERT STATEMENTS --- --\\n\")\n",
    "        outfile.write('\\n'.join(property_amenities_sql) + '\\n')\n",
    "    \n",
    "    print(f\"SQL statements have been successfully exported to '{output_filename}'.\")\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "537d0f8c-af90-463c-bf02-2211136cd2f9",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 50,
   "id": "46e423c8-f6a7-446f-9769-b41d10945d06",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "SQL statements have been successfully exported to 'insert_data.sql'.\n"
     ]
    }
   ],
   "source": [
    "import csv\n",
    "import pandas as pd\n",
    "\n",
    "def prepare_data_for_normalized_schema(csv_file):pd.read_csv('german_clean_immo_data.csv')\n",
    "    \n",
    "    \n",
    "    # Dictionaries and sets to keep track of unique records and primary keys\n",
    "locations = {}\n",
    "amenities = {}\n",
    "    \n",
    "    # Counters for generating new primary keys\n",
    "location_id_counter = 1\n",
    "property_id_counter = 1\n",
    "amenity_id_counter = 1\n",
    "    \n",
    "    # Lists to store the SQL statements\n",
    "location_sql = []\n",
    "amenity_sql = []\n",
    "property_sql = []\n",
    "rent_sql = []\n",
    "condition_sql = []\n",
    "property_amenities_sql = []\n",
    "\n",
    "    # Hardcoded list of amenity columns from the CSV\n",
    "amenity_cols = [\n",
    "    ('has_kitchen', 'has_kitchen'),\n",
    "    ('has_cellar', 'has_cellar'),\n",
    "    ('has_lift', 'has_lift'),\n",
    "    ('has_balcony', 'has_balcony'),\n",
    "    ('has_garden', 'has_garden')\n",
    "]\n",
    "\n",
    "    # Pre-populate amenities lookup table to avoid multiple inserts\n",
    "for col_name, amenity_name in amenity_cols:\n",
    "    amenities[amenity_name] = amenity_id_counter\n",
    "    amenity_sql.append(f\"INSERT INTO Amenities (amenity_id, amenity_name) VALUES ({amenity_id_counter}, '{amenity_name}');\")\n",
    "    amenity_id_counter += 1\n",
    "\n",
    "with open(csv_file, mode='r', encoding='utf-8') as file:\n",
    "    reader = csv.DictReader(file)\n",
    "    for row in reader:\n",
    "            # --- Location Data ---\n",
    "        location_key = (row['geo_district'], row['region_state'])\n",
    "        if location_key not in locations:\n",
    "            locations[location_key] = location_id_counter\n",
    "            location_sql.append(\n",
    "                f\"INSERT INTO Location (location_id, geo_district, region_state) VALUES ({location_id_counter}, '{row['geo_district']}', '{row['region_state']}');\"\n",
    "            )\n",
    "            current_location_id = location_id_counter\n",
    "            location_id_counter += 1\n",
    "        else:\n",
    "            current_location_id = locations[location_key]\n",
    "\n",
    "            # --- Property Data ---\n",
    "        property_sql.append(\n",
    "            f\"INSERT INTO Property (property_id, flat_type, number_of_rooms, year_constructed, living_space, listing_date, location_id) VALUES \"\n",
    "            f\"({property_id_counter}, '{row['flat_type']}', {row['number_of_rooms']}, {row['year_constructed']}, {row['living_space']}, '{row['listing_date']}', {current_location_id});\"\n",
    "        )\n",
    "\n",
    "            # --- Rent Data ---\n",
    "            # NOTE: We use the same ID for rent_id and property_id to maintain the one-to-one relationship\n",
    "        rent_sql.append(\n",
    "            f\"INSERT INTO Rent (rent_id, base_rent, service_charge, total_rent, property_id) VALUES \"\n",
    "            f\"({property_id_counter}, {row['base_rent']}, {row['service_charge']}, {row['total_rent']}, {property_id_counter});\"\n",
    "        )\n",
    "\n",
    "            # --- Condition Data ---\n",
    "            # NOTE: We use the same ID for condition_id and property_id for the one-to-one relationship\n",
    "        condition_sql.append(\n",
    "            f\"INSERT INTO `Condition` (condition_id, `condition`, interior_quality, newly_constructed, property_id) VALUES \"\n",
    "            f\"({property_id_counter}, '{row['condition']}', '{row['interior_quality']}', {row['newly_constructed'].upper()}, {property_id_counter});\"\n",
    "        )\n",
    "            \n",
    "            # --- Property_Amenities Data ---\n",
    "        for col_name, amenity_name in amenity_cols:\n",
    "            if row[col_name].upper() == 'TRUE':\n",
    "                current_amenity_id = amenities[amenity_name]\n",
    "                property_amenities_sql.append(\n",
    "                    f\"INSERT INTO Property_Amenities (property_id, amenity_id) VALUES ({property_id_counter}, {current_amenity_id});\"\n",
    "                )\n",
    "\n",
    "        property_id_counter += 1\n",
    "\n",
    "    # Write all the generated SQL statements to a file in the correct order\n",
    "output_filename = \"insert_data.sql\"\n",
    "with open(output_filename, 'w', encoding='utf-8') as outfile:\n",
    "    outfile.write(\"-- --- AMENITIES INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(amenity_sql) + '\\n\\n')\n",
    "    outfile.write(\"-- --- LOCATION INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(location_sql) + '\\n\\n')\n",
    "    outfile.write(\"-- --- PROPERTY INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(property_sql) + '\\n\\n')\n",
    "    outfile.write(\"-- --- RENT INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(rent_sql) + '\\n\\n')\n",
    "    outfile.write(\"-- --- CONDITION INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(condition_sql) + '\\n\\n')\n",
    "    outfile.write(\"-- --- PROPERTY_AMENITIES INSERT STATEMENTS --- --\\n\")\n",
    "    outfile.write('\\n'.join(property_amenities_sql) + '\\n')\n",
    "    \n",
    "print(f\"SQL statements have been successfully exported to '{output_filename}'.\")\n",
    "\n",
    "\n",
    "# Example usage:\n",
    "# Call the function with your CSV file name\n",
    "prepare_data_for_normalized_schema('german_clean_immo_data.csv')\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d72690e4-cfa2-4ee6-8ff2-acf0f4aea3cc",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "venv",
   "language": "python",
   "name": "venv"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
