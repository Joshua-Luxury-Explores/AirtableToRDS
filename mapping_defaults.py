LEXIS_HOTELS_MAPPING = [
    {"rds_column": "hotel_id", "airtable_field": "airtable_record_id", "editable": False, "note": "Airtable record id"},
    {"rds_column": "hotel_name", "airtable_field": "Hotel Name", "editable": True},
    {"rds_column": "hotel_location", "airtable_field": "Primary City", "editable": False, "note": "resolved city name"},
    {"rds_column": "hotel_coordinates", "airtable_field": "Latitude / Longtitude", "editable": False, "note": "computed lat,long"},
    {"rds_column": "website", "airtable_field": "Hotel Website", "editable": True},
    {"rds_column": "country_id", "airtable_field": "Country", "editable": False, "note": "resolved UID"},
    {"rds_column": "city_id", "airtable_field": "Primary City", "editable": False, "note": "resolved UID"},
    {"rds_column": "verification", "airtable_field": "Verified", "editable": True},
    {"rds_column": "amadeus_code", "airtable_field": "Scrapped Amadeus Code", "editable": True},
    {"rds_column": "hotel_category", "airtable_field": "Hotel Category (Star Rating)", "editable": True},
    {"rds_column": "room_count", "airtable_field": "Room Count", "editable": True},
    {"rds_column": "meeting_room_count", "airtable_field": "Meeting Rooms", "editable": True},
    {"rds_column": "max_capacity", "airtable_field": "Max Capacity (Theater)", "editable": True},
    {"rds_column": "cvent_link", "airtable_field": "Cvent Link", "editable": True},
    {"rds_column": "hotel_group_id", "airtable_field": "Global Hotel Chain", "editable": False, "note": "resolved UID"},
    {"rds_column": "hotel_group_name", "airtable_field": "Global Hotel Chain", "editable": False, "note": "resolved name"},
    {"rds_column": "hotel_chain_id", "airtable_field": "Hotel Chain (from Hotel Brands)", "editable": False, "note": "resolved UID"},
    {"rds_column": "hotel_chain_name", "airtable_field": "Hotel Chain (from Hotel Brands)", "editable": False, "note": "resolved name"},
    {"rds_column": "hotel_chain_type", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "hotel_brand_id", "airtable_field": "Hotel Brands", "editable": False, "note": "resolved UID"},
    {"rds_column": "hotel_brand_name", "airtable_field": "Hotel Brands", "editable": False, "note": "resolved name"},
    {"rds_column": "hotel_loyalty_program", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "aditional_emails", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "restaurant_id", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "attraction_id", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "contract_id", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "rating", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "internal_comments", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "create_time", "airtable_field": "Created", "editable": True},
    {"rds_column": "modify_time", "airtable_field": "Last Modified", "editable": True},
    {"rds_column": "create_by", "airtable_field": "Created By", "editable": True},
    {"rds_column": "modify_by", "airtable_field": "Last Modified By", "editable": True},
    {"rds_column": "sublime_preferred_partner", "airtable_field": None, "editable": False, "note": "always false"},
    {"rds_column": "itravelle_preferred_partner", "airtable_field": None, "editable": False, "note": "always false"},
    {"rds_column": "source_table", "airtable_field": None, "editable": False, "note": "constant 'scrapped_hotels'"},
    {"rds_column": "priority", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "unique_row_id", "airtable_field": "Scrapped Amadeus Code", "editable": False, "note": "= amadeus_code (dedup key)"},
    {"rds_column": "secondary_city_id", "airtable_field": "Secondary Cities", "editable": True},
    {"rds_column": "affiliations", "airtable_field": None, "editable": False, "note": "defaults to []"},
    {"rds_column": "hotel_desc", "airtable_field": "Hotel Description", "editable": True},
    {"rds_column": "hotel_amenities", "airtable_field": "Amenities List", "editable": True},
    {"rds_column": "hotel_address", "airtable_field": "Address", "editable": True},
    {"rds_column": "no_of_floors", "airtable_field": "No of Floors", "editable": True},
    {"rds_column": "no_of_restaurants", "airtable_field": "Number of Restaurants", "editable": True},
    {"rds_column": "no_of_pools", "airtable_field": "Number of Pools", "editable": True},
    {"rds_column": "is_gym", "airtable_field": "Gym", "editable": True},
    {"rds_column": "phone", "airtable_field": "Phone", "editable": True},
]

LEXIS_ROOMS_MAPPING = [
    {"rds_column": "hotel_id", "airtable_field": "Hotel", "editable": False, "note": "Airtable linked record id"},
    {"rds_column": "amadeus_code", "airtable_field": "Amadeus Code (from Hotel)", "editable": True},
    {"rds_column": "room_name", "airtable_field": "Room Name", "editable": True},
    {"rds_column": "view_type", "airtable_field": "Room View", "editable": True},
    {"rds_column": "hotel_room_code", "airtable_field": "Hotel Room Code", "editable": True},
    {"rds_column": "gds_room_code", "airtable_field": "GDS Room Code", "editable": True},
    {"rds_column": "beds_layout", "airtable_field": "Beds & Layout", "editable": True},
    {"rds_column": "bed_type", "airtable_field": "Bed Type", "editable": True},
    {"rds_column": "bed_count", "airtable_field": "Bed Count", "editable": True},
    {"rds_column": "extra_beds", "airtable_field": "Extra Beds", "editable": True},
    {"rds_column": "occupancy_adults", "airtable_field": "Occupancy (Adults)", "editable": True},
    {"rds_column": "occupancy_children", "airtable_field": "Occupancy (Children)", "editable": True},
    {"rds_column": "occupancy_infants", "airtable_field": "Occupancy (Infants)", "editable": True},
    {"rds_column": "raw_size", "airtable_field": "Size", "editable": True},
    {"rds_column": "size_sqm", "airtable_field": "Size", "editable": False, "note": "parsed from Size"},
    {"rds_column": "size_sqft", "airtable_field": "Size", "editable": False, "note": "parsed from Size"},
    {"rds_column": "bathroom", "airtable_field": "Bathroom", "editable": True},
    {"rds_column": "in_room_features", "airtable_field": "In-room features", "editable": True},
    {"rds_column": "in_room_amenities", "airtable_field": " In-room amenities", "editable": True},
    {"rds_column": "photos", "airtable_field": "Image Uploads / Captions", "editable": False, "note": "uploaded S3 URLs"},
    {"rds_column": "hotel_name", "airtable_field": None, "editable": False, "note": "no source field"},
    {"rds_column": "chain_brand", "airtable_field": "Chain / Brand (from Hotel)", "editable": True},
    {"rds_column": "verified", "airtable_field": "Verified (from Hotel)", "editable": True},
    {"rds_column": "image_uploads_captions", "airtable_field": "Image Uploads / Captions", "editable": False, "note": "attachment filenames"},
    {"rds_column": "airtable_record_id", "airtable_field": "airtable_record_id", "editable": False},
    {"rds_column": "source_id", "airtable_field": "airtable_record_id", "editable": False},
    {"rds_column": "create_time", "airtable_field": "Created", "editable": True},
    {"rds_column": "create_by", "airtable_field": "Created By", "editable": True},
    {"rds_column": "modify_time", "airtable_field": "Last Modified", "editable": True},
    {"rds_column": "modify_by", "airtable_field": "Last Modified By", "editable": True},
]

DEFAULT_MAPPINGS = {
    "lexis_hotels": LEXIS_HOTELS_MAPPING,
    "lexis_rooms": LEXIS_ROOMS_MAPPING,
}


def get_effective_mapping(table_name, overrides):
    defaults = DEFAULT_MAPPINGS[table_name]
    overrides = overrides or {}
    effective = {}
    for row in defaults:
        col = row["rds_column"]
        if row["editable"] and col in overrides and overrides[col]:
            effective[col] = overrides[col]
        else:
            effective[col] = row["airtable_field"]
    return effective
