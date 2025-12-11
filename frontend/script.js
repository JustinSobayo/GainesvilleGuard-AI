document.addEventListener('DOMContentLoaded', () => {
    // Initialize Map centered on Gainesville, FL
    const map = L.map('map').setView([29.6516, -82.3248], 13);

    // Add OpenStreetMap tiles
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    // UI Controls
    const timeSlider = document.getElementById('time-slider');
    const timeDisplay = document.getElementById('time-display');
    const suggestionsBox = document.getElementById('suggestions');
    let riskLayer = L.layerGroup().addTo(map);

    // Auto-detect time on load
    const now = new Date();
    const currentHour = now.getHours();
    timeSlider.value = currentHour;
    updateTimeDisplay(currentHour);
    fetchRiskData(currentHour);

    // Update Time Display
    timeSlider.addEventListener('input', (e) => {
        const hour = parseInt(e.target.value);
        updateTimeDisplay(hour);
        fetchRiskData(hour); // Debouncing recommended for production
    });

    function updateTimeDisplay(hour) {
        const ampm = hour >= 12 ? 'PM' : 'AM';
        const displayHour = hour % 12 || 12;
        timeDisplay.textContent = `${displayHour}:00 ${ampm}`;
    }

    async function fetchRiskData(hour) {
        suggestionsBox.innerHTML = '<p>Loading safety data...</p>';
        riskLayer.clearLayers();

        try {
            const response = await fetch(`http://localhost:8000/api/risk?hour=${hour}`);
            const data = await response.json();

            if (data.locations && data.locations.length > 0) {
                let listHtml = '<ul>';

                data.locations.forEach(loc => {
                    // Add circle to map
                    const color = loc.risk > 5 ? 'red' : 'orange';
                    L.circle([loc.lat, loc.lon], {
                        color: color,
                        fillColor: color,
                        fillOpacity: 0.5,
                        radius: 300
                    }).addTo(riskLayer)
                        .bindPopup(`<b>${loc.address}</b><br>Risk Score: ${loc.risk}`);

                    listHtml += `<li>⚠️ Avoid <strong>${loc.address}</strong> (Risk: ${loc.risk})</li>`;
                });

                listHtml += '</ul>';
                suggestionsBox.innerHTML = `<h3>Safety Alert (${timeDisplay.textContent})</h3>${listHtml}`;
            } else {
                suggestionsBox.innerHTML = `<h3>Safety Status</h3><p>No high-risk areas detected for this time.</p>`;
            }

        } catch (error) {
            console.error(error);
            suggestionsBox.innerHTML = `<p>Error connecting to server. Ensure backend is running.</p>`;
        }
    }
});
