#!/usr/bin/env python3
"""
Fetch and parse FAA aviation data for the Burlington VT (KBTV) 50nm radius area.
Outputs JSON files consumed by the CesiumJS 3D viewer.

ARINC 424 format reference (132-char fixed-width records):
  [0]:     Record type ('S' standard)
  [1:4]:   Area code ('USA')
  [4]:     Section code ('P'=airport, 'D'=navaid, 'E'=enroute)
  [5]:     Subsection (for D/E records) or blank (for P records)
  For P records, subsection is at [12]:
    A=airport ref, C=terminal waypoint, D=SID, E=STAR, F=approach
"""

import json
import math
import os
import urllib.request
import zipfile
import io
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# KBTV coordinates
KBTV_LAT = 44.471861
KBTV_LON = -73.153278
RADIUS_NM = 50


def distance_nm(lat1, lon1, lat2, lon2):
    """Haversine distance in nautical miles."""
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def parse_lat(s):
    """Parse ARINC 424 latitude like 'N44281904' -> decimal degrees."""
    s = s.strip()
    if not s or len(s) < 9:
        return None
    hem = s[0]
    if hem not in ('N', 'S'):
        return None
    try:
        deg = int(s[1:3])
        mins = int(s[3:5])
        secs = int(s[5:7])
        hsecs = int(s[7:9])
    except (ValueError, IndexError):
        return None
    val = deg + mins / 60.0 + (secs + hsecs / 100.0) / 3600.0
    return -val if hem == 'S' else val


def parse_lon(s):
    """Parse ARINC 424 longitude like 'W073091179' -> decimal degrees."""
    s = s.strip()
    if not s or len(s) < 10:
        return None
    hem = s[0]
    if hem not in ('E', 'W'):
        return None
    try:
        deg = int(s[1:4])
        mins = int(s[4:6])
        secs = int(s[6:8])
        hsecs = int(s[8:10])
    except (ValueError, IndexError):
        return None
    val = deg + mins / 60.0 + (secs + hsecs / 100.0) / 3600.0
    return -val if hem == 'W' else val


def download_cifp():
    """Download FAA CIFP data (current cycle ZIP)."""
    cifp_path = DATA_DIR / "FAACIFP18"
    if cifp_path.exists():
        print("CIFP data already downloaded.")
        return cifp_path

    urls = [
        "https://aeronav.faa.gov/Upload_313-d/cifp/CIFP_260219.zip",
        "https://aeronav.faa.gov/Upload_313-d/cifp/CIFP_260319.zip",
    ]
    for url in urls:
        print(f"Downloading FAA CIFP data from {url}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=180) as resp:
                zip_data = resp.read()
            print(f"Downloaded {len(zip_data)} bytes, extracting...")
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                for name in zf.namelist():
                    if "FAACIFP18" in name.upper() or (
                        not '.' in name.split('/')[-1] and zf.getinfo(name).file_size > 1000000
                    ):
                        data = zf.read(name)
                        cifp_path.write_bytes(data)
                        print(f"Extracted {name}: {len(data)} bytes")
                        return cifp_path
                largest = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
                data = zf.read(largest)
                cifp_path.write_bytes(data)
                print(f"Extracted {largest}: {len(data)} bytes")
                return cifp_path
        except Exception as e:
            print(f"Error with {url}: {e}")
    return None


# ============================================================
# Phase 1: Build coordinate database (fixes, navaids, airports)
# ============================================================

def build_fix_database(cifp_path):
    """Build a comprehensive lat/lon lookup for all fixes, navaids, airports."""
    fix_db = {}  # id -> (lat, lon, type, name)
    airports = {}  # icao -> {id, name, lat, lon, elevation, ...}

    print("Building fix coordinate database...")
    with open(cifp_path, 'r', errors='replace') as f:
        for line in f:
            if len(line) < 51:
                continue

            sec = line[4:5]

            # --- Airport reference points (sec P, subsec A at pos 12) ---
            if sec == 'P' and len(line) > 12 and line[12] == 'A':
                icao = line[6:10].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None:
                    name = line[93:123].strip() if len(line) > 123 else ""
                    elev_str = line[56:61].strip()
                    elev = 0
                    try:
                        elev = int(elev_str)
                    except ValueError:
                        pass
                    fix_db[icao] = (lat, lon, "APT", name)
                    # Also store without K prefix, but DON'T overwrite navaids
                    if icao.startswith('K') and len(icao) == 4:
                        short = icao[1:]
                        if short not in fix_db or fix_db[short][2] == "APT":
                            fix_db[short] = (lat, lon, "APT", name)
                    dist = distance_nm(KBTV_LAT, KBTV_LON, lat, lon)
                    if dist <= RADIUS_NM:
                        airports[icao] = {
                            "id": icao,
                            "name": name,
                            "lat": lat,
                            "lon": lon,
                            "elevation": elev,
                            "distance_nm": round(dist, 1)
                        }

            # --- Terminal waypoints (sec P, subsec C at pos 12) ---
            elif sec == 'P' and len(line) > 12 and line[12] == 'C':
                wp_id = line[13:18].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None and wp_id:
                    wp_type_code = line[26:27] if len(line) > 26 else ""
                    if wp_type_code == 'W':
                        wtype = "RNAV"
                    elif wp_type_code == 'C':
                        wtype = "REP-COMPULSORY"
                    elif wp_type_code == 'R':
                        wtype = "REP-NONCOMPULSORY"
                    else:
                        wtype = "FIX"
                    fix_db[wp_id] = (lat, lon, wtype, "")

            # --- Runway records (sec P, subsec G at pos 12) ---
            elif sec == 'P' and len(line) > 12 and line[12] == 'G':
                apt_code = line[6:10].strip()
                rw_id = line[13:18].strip()  # e.g. RW15, RW33
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None and rw_id:
                    # Store with airport-qualified key to avoid cross-airport collisions
                    fix_db[f"{apt_code}:{rw_id}"] = (lat, lon, "RWY", "")

            # --- VHF Navaids (sec D, subsec ' ' at pos 5) ---
            elif sec == 'D' and line[5] == ' ':
                nav_id = line[13:17].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None and nav_id:
                    freq_str = line[22:27].strip()
                    name = line[93:123].strip() if len(line) > 123 else ""
                    # Determine navaid subtype from class field (pos 27-29)
                    nav_class = line[27:31] if len(line) > 30 else ""
                    has_vor = 'V' in nav_class
                    has_dme = 'D' in nav_class
                    has_tacan = 'T' in nav_class
                    if has_vor and has_tacan:
                        nav_type = "VORTAC"
                    elif has_vor and has_dme:
                        nav_type = "VOR-DME"
                    elif has_vor:
                        nav_type = "VOR"
                    elif has_tacan:
                        nav_type = "TACAN"
                    elif has_dme:
                        nav_type = "DME"
                    else:
                        nav_type = "VOR"
                    fix_db[nav_id] = (lat, lon, nav_type, name)

            # --- NDB Navaids (sec D, subsec 'B' at pos 5) ---
            elif sec == 'D' and line[5] == 'B':
                nav_id = line[13:17].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None and nav_id:
                    name = line[93:123].strip() if len(line) > 123 else ""
                    fix_db[nav_id] = (lat, lon, "NDB", name)

            # --- Enroute waypoints (sec E, subsec A at pos 5) ---
            elif sec == 'E' and line[5] == 'A':
                wp_id = line[13:18].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is not None and lon is not None and wp_id:
                    # Waypoint type at position 26: W=RNAV, C=compulsory, R=named intersection
                    wp_type_code = line[26:27] if len(line) > 26 else ""
                    if wp_type_code == 'W':
                        wtype = "RNAV"
                    elif wp_type_code == 'C':
                        wtype = "REP-COMPULSORY"
                    elif wp_type_code == 'R':
                        wtype = "REP-NONCOMPULSORY"
                    else:
                        wtype = "FIX"
                    if wp_id not in fix_db:  # don't overwrite navaid coords
                        fix_db[wp_id] = (lat, lon, wtype, "")

    print(f"  Fix database: {len(fix_db)} entries")
    print(f"  Airports within {RADIUS_NM}nm: {len(airports)}")
    return fix_db, airports


# ============================================================
# Phase 2: Parse navaids within radius
# ============================================================

def parse_navaids(cifp_path, fix_db):
    """Extract navaids near KBTV."""
    navaids = []
    print("Finding navaids in area...")
    with open(cifp_path, 'r', errors='replace') as f:
        for line in f:
            if len(line) < 51:
                continue
            sec = line[4:5]
            sub = line[5:6]

            if sec == 'D' and sub in (' ', 'B'):
                nav_id = line[13:17].strip()
                lat = parse_lat(line[32:41])
                lon = parse_lon(line[41:51])
                if lat is None or lon is None:
                    continue
                dist = distance_nm(KBTV_LAT, KBTV_LON, lat, lon)
                if dist > RADIUS_NM:
                    continue

                # Check for continuation records (skip them)
                cont = line[21:22]
                if cont not in ('0', '1', ' '):
                    continue

                freq_str = line[22:27].strip()
                name = line[93:123].strip() if len(line) > 123 else ""
                # Use detailed type from fix_db if available
                if nav_id in fix_db:
                    nav_type = fix_db[nav_id][2]
                else:
                    nav_type = "VOR" if sub == ' ' else "NDB"

                navaids.append({
                    "id": nav_id,
                    "type": nav_type,
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "freq": freq_str,
                    "distance_nm": round(dist, 1)
                })

    # Deduplicate by ID
    seen = set()
    unique = []
    for n in navaids:
        if n["id"] not in seen:
            seen.add(n["id"])
            unique.append(n)
    print(f"  Found {len(unique)} navaids")
    return unique


# ============================================================
# Phase 3: Parse airways
# ============================================================

def parse_airways(cifp_path, fix_db):
    """Parse Victor airways, T-routes, J-routes, Q-routes near KBTV."""
    airways = defaultdict(list)
    print("Parsing airways...")

    with open(cifp_path, 'r', errors='replace') as f:
        for line in f:
            if len(line) < 80:
                continue
            if line[4:6] != 'ER':  # Enroute Routes
                continue

            route_id = line[13:18].strip()
            seq_str = line[25:29].strip()
            fix_id = line[29:34].strip()

            if not fix_id or not route_id:
                continue

            # Look up fix coordinates
            coords = fix_db.get(fix_id)
            if not coords:
                continue

            lat, lon = coords[0], coords[1]
            dist = distance_nm(KBTV_LAT, KBTV_LON, lat, lon)

            # Parse altitude limits from the line
            # MEA is typically around positions 83-88, MAA around 88-93
            min_alt = None
            max_alt = None
            try:
                min_alt_str = line[83:88].strip()
                if min_alt_str and min_alt_str.isdigit():
                    min_alt = int(min_alt_str)
            except (ValueError, IndexError):
                pass
            try:
                max_alt_str = line[88:93].strip()
                if max_alt_str and max_alt_str.isdigit():
                    max_alt = int(max_alt_str)
            except (ValueError, IndexError):
                pass

            airways[route_id].append({
                "fix": fix_id,
                "lat": lat,
                "lon": lon,
                "seq": seq_str,
                "min_alt": min_alt,
                "max_alt": max_alt,
                "dist_from_btv": round(dist, 1)
            })

    # Filter: keep airways that have at least one fix within radius + buffer
    result = {}
    for route_id, fixes in airways.items():
        has_nearby = any(f["dist_from_btv"] <= RADIUS_NM for f in fixes)
        if has_nearby:
            # Sort by sequence
            fixes.sort(key=lambda x: x.get("seq", "0000"))
            # Trim to fixes within extended radius (show full airway segments)
            trimmed = [f for f in fixes if f["dist_from_btv"] <= RADIUS_NM + 30]
            if len(trimmed) >= 2:
                # Clean up - remove dist_from_btv from output
                for f in trimmed:
                    del f["dist_from_btv"]
                result[route_id] = {
                    "id": route_id,
                    "type": "J" if route_id.startswith("J") else
                            "V" if route_id.startswith("V") else
                            "T" if route_id.startswith("T") else
                            "Q" if route_id.startswith("Q") else "OTHER",
                    "fixes": trimmed
                }

    print(f"  Found {len(result)} airways")
    return result


# ============================================================
# Phase 4: Parse instrument procedures
# ============================================================

def parse_procedures(cifp_path, airports, fix_db):
    """Parse SIDs, STARs, and approaches for airports within radius."""
    apt_ids = set(airports.keys())
    procedures = {"approaches": [], "departures": [], "arrivals": []}

    # Group procedure lines by airport+subsection+procedure name
    proc_lines = defaultdict(list)

    print("Parsing instrument procedures...")
    with open(cifp_path, 'r', errors='replace') as f:
        for line in f:
            if len(line) < 60:
                continue
            if line[4:5] != 'P':
                continue
            if len(line) <= 12:
                continue

            sub = line[12]
            if sub not in ('D', 'E', 'F'):
                continue

            apt_id = line[6:10].strip()
            if apt_id not in apt_ids:
                continue

            proc_name = line[13:19].strip()
            key = f"{apt_id}|{sub}|{proc_name}"
            proc_lines[key].append(line)

    # Now parse each procedure
    for key, lines in proc_lines.items():
        apt_id, sub_code, proc_name = key.split('|')
        proc_type = {'D': 'departures', 'E': 'arrivals', 'F': 'approaches'}[sub_code]

        legs = []
        for line in lines:
            route_type = line[19:20] if len(line) > 19 else ""
            transition = line[20:25].strip() if len(line) > 24 else ""
            seq_str = line[26:29].strip() if len(line) > 28 else ""
            fix_id = line[29:34].strip() if len(line) > 33 else ""

            # Path terminator (2 chars)
            path_term = ""
            if len(line) > 48:
                path_term = line[47:49].strip()

            # Get fix coordinates from database
            # For runway fixes (RW##), use airport-qualified key
            lat, lon = None, None
            if fix_id:
                if fix_id.startswith('RW'):
                    rw_key = f"{apt_id}:{fix_id}"
                    if rw_key in fix_db:
                        lat, lon = fix_db[rw_key][0], fix_db[rw_key][1]
                if lat is None and fix_id in fix_db:
                    lat, lon = fix_db[fix_id][0], fix_db[fix_id][1]

            # Parse altitude - scan for altitude pattern in the line
            # The altitude descriptor and values are in the later part of the record
            alt1 = None
            alt2 = None
            alt_desc = ""

            # Altitude fields are typically around positions 82-94
            if len(line) > 94:
                alt_desc = line[82:83].strip()
                a1 = line[84:89].strip()
                a2 = line[89:94].strip()

                # Handle FL (flight level) notation
                if a1:
                    if a1.startswith('FL'):
                        try:
                            alt1 = int(a1[2:]) * 100
                        except ValueError:
                            pass
                    elif a1.lstrip('0').isdigit() or (a1 and a1.isdigit()):
                        try:
                            alt1 = int(a1)
                        except ValueError:
                            pass

                if a2:
                    if a2.startswith('FL'):
                        try:
                            alt2 = int(a2[2:]) * 100
                        except ValueError:
                            pass
                    elif a2.lstrip('0').isdigit() or (a2 and a2.isdigit()):
                        try:
                            alt2 = int(a2)
                        except ValueError:
                            pass

            leg = {
                "fix": fix_id,
                "path_term": path_term,
                "seq": seq_str,
                "transition": transition,
                "route_type": route_type,
            }
            if lat is not None:
                leg["lat"] = round(lat, 6)
            if lon is not None:
                leg["lon"] = round(lon, 6)
            if alt1 is not None:
                leg["alt1"] = alt1
            if alt2 is not None:
                leg["alt2"] = alt2
            if alt_desc:
                leg["alt_desc"] = alt_desc

            legs.append(leg)

        if legs:
            # Determine a readable procedure type name
            type_prefix = proc_name[0] if proc_name else ""
            if sub_code == 'F':
                if type_prefix == 'I':
                    proc_readable = f"ILS"
                elif type_prefix == 'L':
                    proc_readable = f"LOC"
                elif type_prefix == 'R':
                    proc_readable = f"RNAV (GPS)"
                elif type_prefix == 'V':
                    proc_readable = f"VOR"
                elif type_prefix == 'N':
                    proc_readable = f"NDB"
                elif type_prefix == 'S':
                    proc_readable = f"VOR/DME"
                elif type_prefix == 'H':
                    proc_readable = f"RNAV (RNP)"
                elif type_prefix == 'P':
                    proc_readable = f"GPS"
                else:
                    proc_readable = proc_name
            else:
                proc_readable = proc_name

            procedures[proc_type].append({
                "airport": apt_id,
                "name": proc_name,
                "readable_name": proc_readable,
                "type": proc_type,
                "legs": legs
            })

    print(f"  Approaches: {len(procedures['approaches'])}")
    print(f"  Departures: {len(procedures['departures'])}")
    print(f"  Arrivals: {len(procedures['arrivals'])}")
    return procedures


# ============================================================
# Airspace boundaries
# ============================================================

def simplify_polygon(coords, tolerance=0.002):
    """Simplify a polygon using Douglas-Peucker-like point reduction.
    tolerance is in degrees (~0.002 deg ≈ 0.12nm)."""
    if len(coords) <= 20:
        return coords
    # Keep every Nth point plus ensure we keep corners (high angle change)
    simplified = [coords[0]]
    for i in range(1, len(coords) - 1):
        # Distance from line between prev kept point and current point
        prev = simplified[-1]
        curr = coords[i]
        dx = curr[0] - prev[0]
        dy = curr[1] - prev[1]
        dist = math.sqrt(dx*dx + dy*dy)
        if dist >= tolerance:
            simplified.append(curr)
    simplified.append(coords[-1])
    return simplified


def fetch_class_esfc_boundaries():
    """Fetch Class E Surface area boundaries from FAA ArcGIS API."""
    print("Fetching Class E Surface boundaries from FAA...")
    url = (
        "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/"
        "Class_Airspace/FeatureServer/0/query?"
        "where=CLASS%3D%27E%27+AND+LOWER_VAL%3D0"
        "&outFields=NAME"
        "&outSR=4326"
        "&geometry=-74.5,43.0,-71.5,45.5"
        "&geometryType=esriGeometryEnvelope"
        "&inSR=4326"
        "&spatialRel=esriSpatialRelIntersects"
        "&f=json"
    )
    areas = []
    try:
        from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        features = data.get("features", [])
        print(f"  Found {len(features)} Class E Surface areas")

        for feat in features:
            name = feat.get("attributes", {}).get("NAME", "")
            geom = feat.get("geometry", {})
            rings = geom.get("rings", [])
            if not rings:
                continue
            ring = rings[0]
            if len(ring) < 4:
                continue
            print(f"  {name}: {len(ring)} pts")
            try:
                sp = ShapelyPolygon(ring)
                if not sp.is_valid:
                    sp = sp.buffer(0)
                if sp.is_valid:
                    coords = list(sp.simplify(0.001).exterior.coords)
                    poly = [[round(c[1], 5), round(c[0], 5)] for c in coords]
                    areas.append({
                        "name": f"{name.title()} Class E Surface",
                        "type": "E-SFC",
                        "center_lat": KBTV_LAT,
                        "center_lon": KBTV_LON,
                        "radius_nm": 50,
                        "floor": 0, "floor_type": "SFC",
                        "ceiling": 700, "ceiling_type": "AGL",
                        "shape": "polygon",
                        "polygon": poly,
                        "color": [128, 0, 128]
                    })
            except Exception:
                pass
    except Exception as e:
        print(f"  Error fetching E-SFC boundaries: {e}")
        print("  Using fallback circles for E-SFC areas")
        # Fallback circles
        areas.append({
            "name": "Plattsburgh Class E Surface",
            "type": "E-SFC",
            "center_lat": 44.6509, "center_lon": -73.4681,
            "radius_nm": 4.3,
            "floor": 0, "floor_type": "SFC",
            "ceiling": 700, "ceiling_type": "AGL",
            "shape": "circle",
            "color": [128, 0, 128]
        })
        areas.append({
            "name": "Montpelier Class E Surface",
            "type": "E-SFC",
            "center_lat": 44.2036, "center_lon": -72.5623,
            "radius_nm": 4.3,
            "floor": 0, "floor_type": "SFC",
            "ceiling": 700, "ceiling_type": "AGL",
            "shape": "circle",
            "color": [128, 0, 128]
        })
    return areas


def fetch_class_e700_boundaries():
    """Fetch Class E 700ft AGL transition area boundaries from FAA ArcGIS API
    and merge them into a single unified polygon."""
    print("Fetching Class E 700ft AGL boundaries from FAA...")
    url = (
        "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/"
        "Class_Airspace/FeatureServer/0/query?"
        "where=CLASS%3D%27E%27+AND+LOWER_VAL%3D700"
        "&outFields=NAME"
        "&outSR=4326"
        "&geometry=-74.5,43.0,-71.5,45.5"
        "&geometryType=esriGeometryEnvelope"
        "&inSR=4326"
        "&spatialRel=esriSpatialRelIntersects"
        "&f=json"
    )
    areas = []
    try:
        from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon
        from shapely.ops import unary_union

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        features = data.get("features", [])
        print(f"  Found {len(features)} Class E5 areas")

        # Collect all polygons as shapely objects
        shapely_polys = []
        for feat in features:
            name = feat.get("attributes", {}).get("NAME", "")
            geom = feat.get("geometry", {})
            rings = geom.get("rings", [])
            if not rings:
                continue
            ring = rings[0]
            if len(ring) < 4:
                continue
            try:
                sp = ShapelyPolygon(ring)
                if sp.is_valid:
                    shapely_polys.append(sp)
                else:
                    sp = sp.buffer(0)  # fix invalid geometry
                    if sp.is_valid:
                        shapely_polys.append(sp)
            except Exception:
                pass
            print(f"  {name}: {len(ring)} pts")

        # Merge all polygons into one unified shape
        print(f"  Merging {len(shapely_polys)} polygons...")
        merged = unary_union(shapely_polys)

        # Extract polygon(s) from the result
        def extract_polygons(geom):
            """Convert shapely geometry to list of [lat, lon] polygons."""
            polys = []
            if isinstance(geom, ShapelyPolygon):
                coords = list(geom.simplify(0.001).exterior.coords)
                poly = [[round(c[1], 5), round(c[0], 5)] for c in coords]
                polys.append(poly)
            elif isinstance(geom, MultiPolygon):
                for g in geom.geoms:
                    coords = list(g.simplify(0.001).exterior.coords)
                    poly = [[round(c[1], 5), round(c[0], 5)] for c in coords]
                    polys.append(poly)
            return polys

        merged_polys = extract_polygons(merged)
        print(f"  Result: {len(merged_polys)} polygon(s)")
        for i, poly in enumerate(merged_polys):
            print(f"    Polygon {i}: {len(poly)} points")
            areas.append({
                "name": "Class E 700ft AGL",
                "type": "E-700",
                "center_lat": KBTV_LAT,
                "center_lon": KBTV_LON,
                "radius_nm": 50,
                "floor": 700, "floor_type": "AGL",
                "ceiling": 1200, "ceiling_type": "AGL",
                "shape": "polygon",
                "polygon": poly,
                "color": [128, 0, 128]
            })
    except Exception as e:
        print(f"  Error fetching E-700 data: {e}")
        import traceback; traceback.print_exc()
        areas.append({
            "name": "Class E 700ft AGL (approximate)",
            "type": "E-700",
            "center_lat": KBTV_LAT,
            "center_lon": KBTV_LON,
            "radius_nm": 35,
            "floor": 700, "floor_type": "AGL",
            "ceiling": 1200, "ceiling_type": "AGL",
            "shape": "circle",
            "color": [128, 0, 128],
        })
    return areas


def fetch_class_e1200_boundaries():
    """Fetch Class E 1200ft AGL boundaries from FAA ArcGIS API
    and merge them into a single unified polygon."""
    print("Fetching Class E 1200ft AGL boundaries from FAA...")
    url = (
        "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/"
        "Class_Airspace/FeatureServer/0/query?"
        "where=CLASS%3D%27E%27+AND+LOWER_VAL%3D1200"
        "&outFields=NAME"
        "&outSR=4326"
        "&geometry=-74.5,43.0,-71.5,45.5"
        "&geometryType=esriGeometryEnvelope"
        "&inSR=4326"
        "&spatialRel=esriSpatialRelIntersects"
        "&f=json"
    )
    areas = []
    try:
        from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon
        from shapely.ops import unary_union

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        features = data.get("features", [])
        print(f"  Found {len(features)} Class E 1200ft areas")

        shapely_polys = []
        for feat in features:
            name = feat.get("attributes", {}).get("NAME", "")
            geom = feat.get("geometry", {})
            rings = geom.get("rings", [])
            if not rings:
                continue
            ring = rings[0]
            if len(ring) < 4:
                continue
            try:
                sp = ShapelyPolygon(ring)
                if sp.is_valid:
                    shapely_polys.append(sp)
                else:
                    sp = sp.buffer(0)
                    if sp.is_valid:
                        shapely_polys.append(sp)
            except Exception:
                pass
            print(f"  {name}: {len(ring)} pts")

        print(f"  Merging {len(shapely_polys)} polygons...")
        merged = unary_union(shapely_polys)

        def extract_polygons(geom):
            polys = []
            if isinstance(geom, ShapelyPolygon):
                coords = list(geom.simplify(0.001).exterior.coords)
                poly = [[round(c[1], 5), round(c[0], 5)] for c in coords]
                polys.append(poly)
            elif isinstance(geom, MultiPolygon):
                for g in geom.geoms:
                    coords = list(g.simplify(0.001).exterior.coords)
                    poly = [[round(c[1], 5), round(c[0], 5)] for c in coords]
                    polys.append(poly)
            return polys

        merged_polys = extract_polygons(merged)
        print(f"  Result: {len(merged_polys)} polygon(s)")
        for i, poly in enumerate(merged_polys):
            print(f"    Polygon {i}: {len(poly)} points")
            areas.append({
                "name": "Class E 1200ft AGL",
                "type": "E-1200",
                "center_lat": KBTV_LAT,
                "center_lon": KBTV_LON,
                "radius_nm": 50,
                "floor": 1200, "floor_type": "AGL",
                "ceiling": 18000,
                "shape": "polygon",
                "polygon": poly,
                "color": [128, 0, 128]
            })
    except Exception as e:
        print(f"  Error fetching E-1200 data: {e}")
        import traceback; traceback.print_exc()
    return areas


def get_airspace_data():
    """Define airspace boundaries for the KBTV area from published data."""
    airspace = []

    # KBTV Class C — non-overlapping altitude slices (wedding cake)
    import math as _m
    clat, clon = 44.471861, -73.153278
    r_outer = 10 / 60.0  # 10nm in degrees latitude
    r_inner = 5 / 60.0   # 5nm in degrees latitude
    cos_lat = _m.cos(_m.radians(clat))

    def make_arc(radius_deg, start_bearing, end_bearing, npts=36):
        """Generate arc points at given radius from start to end bearing."""
        pts = []
        for i in range(npts + 1):
            angle = _m.radians(start_bearing + (end_bearing - start_bearing) * i / npts)
            lat = clat + radius_deg * _m.cos(angle)
            lon = clon + (radius_deg / cos_lat) * _m.sin(angle)
            pts.append([round(lat, 6), round(lon, 6)])
        return pts

    def make_semicircle_closed(radius_deg, start_bearing, end_bearing, npts=36):
        """Generate a closed semicircle polygon (arc + line through center)."""
        pts = make_arc(radius_deg, start_bearing, end_bearing, npts)
        pts.append([round(clat, 6), round(clon, 6)])
        return pts

    def make_full_circle(radius_deg, npts=72):
        """Generate a full circle polygon."""
        return make_arc(radius_deg, 0, 360, npts)

    # Slice 1: Inner 5nm circle, SFC to 1,500 MSL (below lowest shelf)
    airspace.append({
        "name": "Burlington Class C - Core (SFC-1500)",
        "type": "C",
        "center_lat": clat,
        "center_lon": clon,
        "radius_nm": 5,
        "floor": 0, "floor_type": "SFC",
        "ceiling": 1500,
        "shape": "circle",
        "color": [0, 0, 255]
    })

    # Slice 2a: West 10nm semicircle (full), 1,500 to 2,200 MSL
    airspace.append({
        "name": "Burlington Class C - West mid (1500-2200)",
        "type": "C",
        "center_lat": clat,
        "center_lon": clon,
        "radius_nm": 10,
        "floor": 1500, "floor_type": "MSL",
        "ceiling": 2200,
        "shape": "polygon",
        "polygon": make_semicircle_closed(r_outer, 180, 360),
        "color": [0, 0, 255]
    })

    # Slice 2b: East 5nm semicircle (inner only), 1,500 to 2,200 MSL
    airspace.append({
        "name": "Burlington Class C - East mid (1500-2200)",
        "type": "C",
        "center_lat": clat,
        "center_lon": clon,
        "radius_nm": 5,
        "floor": 1500, "floor_type": "MSL",
        "ceiling": 2200,
        "shape": "polygon",
        "polygon": make_semicircle_closed(r_inner, 0, 180),
        "color": [0, 0, 255]
    })

    # Slice 3: Full 10nm circle, 2,200 to 4,400 MSL (top of cake)
    airspace.append({
        "name": "Burlington Class C - Top (2200-4400)",
        "type": "C",
        "center_lat": clat,
        "center_lon": clon,
        "radius_nm": 10,
        "floor": 2200, "floor_type": "MSL",
        "ceiling": 4400,
        "shape": "polygon",
        "polygon": make_full_circle(r_outer),
        "color": [0, 0, 255]
    })

    # Class E Surface areas - fetch from FAA ArcGIS
    esfc_areas = fetch_class_esfc_boundaries()
    for area in esfc_areas:
        airspace.append(area)

    # Class E 700ft AGL transition areas - fetch from FAA ArcGIS
    e700_areas = fetch_class_e700_boundaries()
    for area in e700_areas:
        airspace.append(area)

    # Class E 1200ft AGL - fetch from FAA ArcGIS
    e1200_areas = fetch_class_e1200_boundaries()
    for area in e1200_areas:
        airspace.append(area)



    return airspace


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("BTV Airspace 3D - Data Fetcher")
    print(f"Center: KBTV ({KBTV_LAT}, {KBTV_LON})")
    print(f"Radius: {RADIUS_NM} nm")
    print("=" * 60)

    cifp_path = download_cifp()
    if not cifp_path:
        print("Failed to download CIFP data.")
        return

    # Phase 1: Build fix database + find airports
    fix_db, airports = build_fix_database(cifp_path)

    # Phase 2: Navaids
    navaids = parse_navaids(cifp_path, fix_db)

    # Phase 3: Airways
    airways = parse_airways(cifp_path, fix_db)

    # Phase 4: Procedures
    procedures = parse_procedures(cifp_path, airports, fix_db)

    # Phase 5: Airspace
    airspace = get_airspace_data()

    # Collect all waypoints/fixes in area with type info
    waypoints = []
    seen_wp = set()
    wp_types = ("RNAV", "REP-COMPULSORY", "REP-NONCOMPULSORY", "FIX", "WPT")
    for fid, (lat, lon, ftype, fname) in fix_db.items():
        if ftype in wp_types and fid not in seen_wp and ':' not in fid:
            dist = distance_nm(KBTV_LAT, KBTV_LON, lat, lon)
            if dist <= RADIUS_NM + 10:
                waypoints.append({
                    "id": fid,
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "type": ftype if ftype != "WPT" else "FIX"
                })
                seen_wp.add(fid)

    # Write output
    output = {
        "center": {"lat": KBTV_LAT, "lon": KBTV_LON},
        "radius_nm": RADIUS_NM,
        "airports": sorted(airports.values(), key=lambda a: a["distance_nm"]),
        "navaids": navaids,
        "waypoints": waypoints,
        "airways": list(airways.values()),
        "procedures": procedures,
        "airspace": airspace
    }

    output_path = DATA_DIR / "btv_airspace_data.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Output: {output_path}")
    print(f"  Airports:    {len(output['airports'])}")
    print(f"  Navaids:     {len(output['navaids'])}")
    print(f"  Waypoints:   {len(output['waypoints'])}")
    print(f"  Airways:     {len(output['airways'])}")
    print(f"  Approaches:  {len(output['procedures']['approaches'])}")
    print(f"  Departures:  {len(output['procedures']['departures'])}")
    print(f"  Arrivals:    {len(output['procedures']['arrivals'])}")
    print(f"  Airspace:    {len(output['airspace'])}")

    # Print airport list
    print(f"\nAirports found:")
    for a in output['airports']:
        print(f"  {a['id']:6s} {a['name'][:40]:40s} {a['distance_nm']:5.1f}nm  elev {a['elevation']}ft")


if __name__ == "__main__":
    main()
