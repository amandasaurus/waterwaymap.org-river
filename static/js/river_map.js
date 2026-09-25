function set_up_map(geojson, bbox) {

  let map = new maplibregl.Map({
    container: "map_div",
    attributionControl: false,  // manually added later (w. date)
    style: {
      version: 8,
      sources: {
          river: {
                  type: 'geojson',
                  data: geojson,
                },
        osmcarto: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          attribution:
            '<a href="https://www.openstreetmap.org/copyright">© OpenStreetMap contributors</a>',
        },
      },
      layers: [
        {
          id: "osmcarto",
          type: "raster",
          source: "osmcarto",
        },
        {
          id: 'river-casing',
          type: 'line',
          source: 'river',
          layout: {
            "line-cap": "round",
            "line-join": "round",
          },
          paint: {
            "line-color": "black",
            "line-width": 6,
          },
        },
        {
          id: 'river-line',
          type: 'line',
          source: 'river',
          layout: {
            "line-cap": "round",
            "line-join": "round",
          },
          paint: {
            "line-color": "#487bb6",
            "line-width": 5,
          },
        },

	  {
			id: 'river-flow',
			type: 'line',
			source: 'river',
			layout: { 'line-join': 'round' }, // keep cap 'butt' so dashes stay crisp
			paint: {
			  'line-color': '#cfeaff',
			  'line-width': 4,
			  'line-opacity': 0.8,
			  'line-dasharray': [0, 4, 3]
			}
	  },

      ],
    },
  });
  // Add geolocate control to the map.
  map.addControl(
    new maplibregl.GeolocateControl({
      positionOptions: {
        enableHighAccuracy: true,
      },
      trackUserLocation: true,
    }),
  );
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.AttributionControl({ compact: false }));

  map.setPadding({ top: 57 });
  map.fitBounds(bbox);


	// Each step shifts the dash pattern slightly along the line
	const dashSeq = [
		[0, 4, 3], [0.5, 4, 2.5], [1, 4, 2], [1.5, 4, 1.5],
		[2, 4, 1], [2.5, 4, 0.5], [3, 4, 0],
		[0, 0.5, 3, 3.5], [0, 1, 3, 3], [0, 1.5, 3, 2.5],
		[0, 2, 3, 2], [0, 2.5, 3, 1.5], [0, 3, 3, 1], [0, 3.5, 3, 0.5]
	];
	let step = -1;
	function animate(t) {
		const next = Math.floor((t / 250) % dashSeq.length); // 60 ms per step; raise to slow down
		if (next !== step) {
			map.setPaintProperty('river-flow', 'line-dasharray', dashSeq[next]);
			step = next;
		}
		requestAnimationFrame(animate);
	}
	requestAnimationFrame(animate);

  var scale = new maplibregl.ScaleControl({
    maxWidth: 200,
    unit: "metric",
  });
  map.addControl(scale);
  
  map.dragRotate.disable();
  map.touchZoomRotate.disableRotation();

  map.fitBounds(bbox);
  document.map = map;
}
