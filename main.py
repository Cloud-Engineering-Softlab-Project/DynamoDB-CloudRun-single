import os

import configuration
app = configuration.init()

from resources.energy_data import EnergyData, ReferenceZones

configuration.api.add_resource(EnergyData, '/energy_data')
configuration.api.add_resource(ReferenceZones, '/ref_zones')

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT")), threaded=True)
