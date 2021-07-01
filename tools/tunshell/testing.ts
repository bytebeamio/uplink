const response = await fetch("https://eu.relay.tunshell.com/api/sessions", {
  method: "POST",
}).then((res) => res.json());

console.log(response);
