function print() {
  // console.log("hello, world")
  // console.log(stationsRetrieval);
  // console.log(availability_allRetrieval);
  // console.log(weatherRetrieval);
}


// ###############################################################################################################################################


async function initMap(operation = '') { // Default value set to 'collect'
  try {
    const { Map } = await google.maps.importLibrary("maps");

    var dublin = { lat: 53.35014, lng: -6.266155 };

    var map = new google.maps.Map(document.getElementById('map'), {
        center: dublin,
        zoom: 14,
        mapTypeControl: false,
        streetViewControl: false,
        fullscreenControl: false,
        styles: [
            {
                featureType: "poi",
                stylers: [{ visibility: "off" }]
            },
            {
                featureType: "transit",
                stylers: [{ visibility: "off" }]
            },
            {
                featureType: "landscape",
                stylers: [{ visibility: "off" }]
            },
            {
                featureType: "administrative",
                elementType: "labels.text.fill"
            },
            {
                featureType: "administrative",
                elementType: "labels.text.stroke",
                stylers: [{ visibility: "off" }]
            },
            {
                featureType: "road",
                elementType: "labels.icon",
                stylers: [{ visibility: "off" }]
            }
        ]
    });

    if (operation === '') {
      
    } else if (operation === 'collect') {
        addMarkers(map, stationsRetrieval, 'collect');
    } else if (operation === 'return') {
        addMarkers(map, stationsRetrieval, 'return');
    }

  } catch (error) {
    console.error('Error initializing map:', error);
  }
}


// ###############################################################################################################################################


async function addMarkers(map, stations, operation) {
  // console.log(stations)
  // console.log(availability_allRetrieval)

  for (const station of stations) {
    try {
      
      // Old method, which caused performance issues:
      // const { availableBikes, availableStands } = await stationCapacity(station.number);
      
      // Revised method:
      const stationAvail = availability_allRetrieval.find((elem) => elem.number === station.number);

      // console.log(stationAvail);

      let iconOptions = {
        path: google.maps.SymbolPath.BACKWARD_CLOSED_ARROW,
        strokeWeight: 3,
        scale: 5,
        strokeColor: "red",
      };
  
      if (operation === 'collect' && parseInt(stationAvail.available_bikes) >= 10) {
        iconOptions.strokeColor = "green";
      } else if (operation === 'return' && parseInt(stationAvail.available_stands) >= 10) {
        iconOptions.strokeColor = "green";
      }
  
      const marker = new google.maps.Marker({
        map: map,
        position: {
          lat: station.position_lat,
          lng: station.position_lng,
        },
        icon: iconOptions,
        title: `Station ${station.number} (${station.name})`,
      });
  
      marker.addListener("click", () => {
        stationDetail(station.number, station.name);
        document.getElementById("predictiveModelResults").innerHTML = "";
        fetchGraphs(station.number);
      });
  
    
  } catch (error) {
        console.error('Error adding marker:', error);
      }
    }


}


// ###############################################################################################################################################


async function stationDetail(num, stationName) {
  const infoDiv = document.getElementById("staticInformation");
  const tripPlanner = document.querySelector(".trip-planner");
  const stationIdDiv = document.getElementById("stationID");

  try {
    const response = await fetch(`/availabilty/${num}`);
    
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const data = await response.json();
    tripPlanner.style.display = 'block';
    planTrip();
    // console.log(data.number);
    // console.log(typeof data);

    infoDiv.innerHTML = `Station Information <br> <br> Station number: ${data.number} <br> Station name: ${stationName} <br> Available bikes: ${data.available_bikes} <br> Available stands: ${data.available_stands} <br> Status: ${data.status}`;
    stationIdDiv.innerHTML = `${data.number}`;
  } catch (error) {
    console.error('Error:', error);
  }

}


// ##############################################################################################################################################


// Function no longer required.

// async function stationCapacity(num) {
//   let x = null;

//   try {
//     const response = await fetch(`/availabilty/${num}`);
    
//     if (!response.ok) {
//         throw new Error('Network response was not ok');
//     }
//     const data = await response.json();
//     availableBikes = data.available_bikes;
//     availableStands = data.available_stands;
//   } catch (error) {
//     console.error('Error:', error);
//   }
//   return { availableBikes, availableStands };
// }


// ##############################################################################################################################################


function displayWeather(weather) {
  let weatherDes = weather['weather_des'];
  let weatherIcon = weather['weather_icon'];
  let weatherTemp = Math.round(weather['temp']);
  let weatherMain = weather['weather_main'];

  document.getElementById("weatherTemp").innerHTML = weatherTemp + "°C";
  document.getElementById("weatherIcon").src = "https://openweathermap.org/img/wn/" + weatherIcon + "@2x.png";
  document.getElementById("weatherIcon").alt = weatherMain;
  document.getElementById("weatherDescription").innerHTML = weatherDes;

}


// ##############################################################################################################################################


document.addEventListener("DOMContentLoaded", function() {
  const buttons = document.querySelectorAll('.statusButton');
  let prevButtonId = null;

  buttons.forEach(button => {
    button.addEventListener('click', function() {

      
      if (prevButtonId !== null) {
        const prevButton = document.getElementById(prevButtonId);
        prevButton.style.backgroundColor = '';
        prevButton.style.textDecoration = '';
      }
      
      this.style.backgroundColor = '#023047';
      this.style.textDecoration = 'underline';
      prevButtonId = this.id;
      document.getElementById("staticInformation").innerHTML = "";
      document.getElementById('graphs').innerHTML = "";

      document.querySelector(".trip-planner").style.display = 'none';
    });
  });
});


// ##############################################################################################################################################


function fetchGraphs(id) {
  const existingGraph = document.getElementById('graphs').querySelector('img');
    if (existingGraph) {
        existingGraph.remove();
    }
  fetch(`/plot/${id}`)
        .then(response => response.blob())
        .then(imageBlob => {
            const img = document.createElement('img');
            img.src = URL.createObjectURL(imageBlob);
            document.getElementById('graphs').appendChild(img);
        })
        .catch(error => console.error('Error fetching graph:', error));
}


// ##############################################################################################################################################
 

function planTrip() {
  var dateDropDown = "";
  var currentDate = new Date();

  var todayNumPython = [6, 0, 1, 2, 3, 4, 5];

  var daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  var nameOfMonth = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'Decmber'];

  for (var i = 1; i <= 3; i++) {
    var nextDayIndex = (currentDate.getDay() + i) % 7;

    var nextDayNumber = todayNumPython[nextDayIndex];
    var nextDayName = daysOfWeek[nextDayIndex];

    var nextDate = new Date(currentDate);
    nextDate.setDate(currentDate.getDate() + i);
    var y = nextDate.getFullYear();
    var m = nextDate.getMonth();
    var d = nextDate.getDate();
    dateDropDown += `<option value="${m+1},${d}"> ${nextDayName}, ${d} ${nameOfMonth[m]} ${y}</option>`;
  }

  document.getElementById("forecastDay").innerHTML = dateDropDown;

}


// ##############################################################################################################################################
 

function dateToday() {
  
  var todayNum = new Date().getDay(); // Get current day of the week (0-6)
  var daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  var dayName = daysOfWeek[todayNum];
   
  var todayFullDate =  new Date();
  var year = todayFullDate.getFullYear();
  var month = todayFullDate.getMonth();
  var date = todayFullDate.getDate(); 
  
  var nameOfMonth = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'Decmber'];
  var monthName = nameOfMonth[month]


  document.getElementById("date").innerHTML = "&nbsp;&nbsp;|&nbsp;&nbsp;" + dayName + ", " + date + " " + monthName + " " + year;

}


// ##############################################################################################################################################


async function searchForecast() {
  
  var selectedStationID = parseInt(document.getElementById("stationID").textContent);
  
  var selectedDay = document.getElementById("forecastDay").value;
  var dateArray = selectedDay.split(',');
  var month = parseInt(dateArray[0]);
  var day = parseInt(dateArray[1]);

  var selectedTime = parseInt(document.getElementById("forecastTime").value);

  // console.log("ID:", selectedStationID);
  // console.log("Month:", month);
  // console.log("Day:", day);
  // console.log("Time:", selectedTime);
  
  const predictiveDiv = document.getElementById("predictiveModelResults");

  try {
    const response = await fetch(`/prediction/${selectedStationID}/${month}/${day}/${selectedTime}`);
    console.log(response)
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const data = await response.json();
    console.log(data)

    predictiveDiv.innerHTML = `Predicted bikes: ${data.bikes_predicted} <br> Predicted stands: ${data.stands_predicted}`;
  } catch (error) {
    console.error('Error:', error);
  }
}