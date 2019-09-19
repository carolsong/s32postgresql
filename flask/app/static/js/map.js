var map, heatmap;

function initMap() {
    $.ajax({
        type: "GET",
        url: "/static/data/sf_incidents_recent.csv",
        dataType: "text",
        success: (data) => { processData(data); }
    });
}

function toggleHeatmap() {
    heatmap.setMap(heatmap.getMap() ? null : map);
}

function processData(allText) {
    var allTextLines = allText.split(/\r\n|\n/);
    let points = [];

    for (var i=1; i<allTextLines.length; i++) {
        var data = allTextLines[i].split(',');
        if (data.length == 2) {
            var latitude = parseFloat(data[0]);
            var longitude = parseFloat(data[1]);
            points.push(new google.maps.LatLng(latitude, longitude));
        }
    }

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 13,
        center: {lat: 37.775, lng: -122.434},
        mapTypeId: 'satellite'
    });

    heatmap = new google.maps.visualization.HeatmapLayer({
        data: points,
        map: map
    });
}

function changeGradient() {
    var gradient = [
        'rgba(0, 255, 255, 0)',
        'rgba(0, 255, 255, 1)',
        'rgba(0, 191, 255, 1)',
        'rgba(0, 127, 255, 1)',
        'rgba(0, 63, 255, 1)',
        'rgba(0, 0, 255, 1)',
        'rgba(0, 0, 223, 1)',
        'rgba(0, 0, 191, 1)',
        'rgba(0, 0, 159, 1)',
        'rgba(0, 0, 127, 1)',
        'rgba(63, 0, 91, 1)',
        'rgba(127, 0, 63, 1)',
        'rgba(191, 0, 31, 1)',
        'rgba(255, 0, 0, 1)'
    ]
    heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
}

function changeRadius() {
    heatmap.set('radius', heatmap.get('radius') ? null : 20);
}

function changeOpacity() {
    heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
}