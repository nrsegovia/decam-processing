from matplotlib import pyplot as plt
import json
from pathlib import Path

local = Path(__file__).parent
json_file = Path(local, "output", "fields_info.json")

if json_file.exists():
    with open(json_file, "r") as f:
        json_data = json.load(f)
else:
    print(f"Expected JSON file does not exist at specified location: {json_file.absolute}\nAborting.")
    exit()

for field in json_data.keys():
    for ccd in json_data[field].keys():
        ra_min, ra_max = json_data[field][ccd]["RA"]
        dec_min, dec_max = json_data[field][ccd]["Dec"]
        ra_center = 0.5 * (ra_max + ra_min)
        dec_center = 0.5 * (dec_max + dec_min)

        x = [ra_min, ra_max, ra_max, ra_min]
        y = [dec_min, dec_min, dec_max, dec_max]

        plt.fill(x,y, fill = False, edgecolor = "k", linewidth = 2, alpha = 0.3)
        plt.text(ra_center, dec_center, ccd, 
             ha='center', va='center',
             fontsize=12, fontweight='bold',
             color='k')
    plt.xlabel("RA")
    plt.ylabel("Dec")
    plt.savefig(Path(local, f"{field}_footprint.png"))

