const $ = (id) => document.getElementById(id);
const uid = "web-" + Math.random().toString(36).slice(2, 10);

async function refresh() {
  const s = await (await fetch("/api/seats")).json();
  $("free").textContent = s.free; $("held").textContent = s.held;
  $("sold").textContent = s.sold; $("total").textContent = s.total;
  $("barfill").style.width = (100 * s.sold / s.total) + "%";
  const soldOut = s.free === 0 && s.held === 0;
  $("buy").hidden = soldOut; $("wl").hidden = !soldOut;
}
function say(t, bad) { const m = $("msg"); m.textContent = t; m.style.color = bad ? "var(--bad)" : "inherit"; }

async function hold() {
  $("buy").disabled = true; say("Holding your seats...");
  const r = await (await fetch("/api/hold", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: uid, seats: +$("n").value }) })).json();
  $("buy").disabled = false;
  if (!r.ok) return say(r.status === "sold_out" ? "Sold out! Try the waitlist." : "Could not hold seats: " + r.status, true);
  showReceipt(r, "Seats held", r.seats.length + " seat(s) held for you", r.amount,
    "Confirm your payment within the hold window.");
  say("Seats held. Paying now...");
  const p = await (await fetch("/api/pay", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reservation_id: r.reservation_id, user_id: uid }) })).json();
  if (p.ok) {
    showReceipt(p, "Booked!", "Booking " + p.booking_id + " confirmed", r.amount, null);
    say("Confirmed. See you at the show!");
  } else {
    say("Payment failed, seats released back to the pool.", true);
    showReceipt({ seats: r.seats, amount: r.amount }, "Released", "Payment failed", r.amount, null);
  }
  refresh();
}

async function waitlistJoin() {
  $("wl").disabled = true;
  const r = await (await fetch("/api/waitlist", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: uid, seats: +$("n").value }) })).json();
  $("wl").disabled = false;
  say(r.ok ? "You are #" + r.position + " in the waitlist." : "Waitlist: " + r.status, !r.ok);
}

function showReceipt(d, title, line, amount, note) {
  const el = $("receipt"); el.hidden = false;
  el.innerHTML = "<h2>" + title + "</h2><table><tr><td>" + line + "</td><td>" +
    d.seats.join(", ") + "</td></tr><tr><td>Amount (app layer)</td><td>Rs " +
    amount.toFixed(0) + "</td></tr></table>" +
    (note ? '<p class="fine">' + note + "</p>" : "");
}

$("buy").onclick = hold; $("wl").onclick = waitlistJoin;
setInterval(refresh, 2000); refresh();
