let weatherObj = JSON.parse(weather);
let weatherDes = weatherObj.weather_des;
let weatherIcon = weatherObj.weather_icon;
let weatherTemp = Math.round(weatherObj.temp);
let weatherMain = weatherObj.weather_main;

document.getElementById("weather-temp").innerHTML = weatherTemp + "°C";
document.getElementById("weather-icon").src = "https://openweathermap.org/img/wn/" + weatherIcon + "@2x.png";
document.getElementById("weather-icon").alt = weatherMain;
document.getElementById("weather-description").innerHTML = weatherDes;