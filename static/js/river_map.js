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
