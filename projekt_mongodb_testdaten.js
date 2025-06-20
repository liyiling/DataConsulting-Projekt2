// Anlage B: Datenimport und Testdatengenerierung (MongoDB)

// 1. Kunden einfügen
db.kunde.insertMany([
  { kundeid: 1, name: "Alpha GmbH", adresse: "Musterstraße 1, Berlin", kontaktdaten: "alpha@example.com" },
  { kundeid: 2, name: "Beta AG", adresse: "Industrieweg 3, Hamburg", kontaktdaten: "beta@example.com" },
  { kundeid: 3, name: "Gamma Co.", adresse: "Techpark 12, München", kontaktdaten: "gamma@example.com" },
  { kundeid: 4, name: "Delta KG", adresse: "Bauhof 5, Köln", kontaktdaten: "delta@example.com" },
  { kundeid: 5, name: "Epsilon SE", adresse: "Werkstraße 7, Frankfurt", kontaktdaten: "epsilon@example.com" },
  { kundeid: 6, name: "Zeta Ltd.", adresse: "Fabrikallee 8, Leipzig", kontaktdaten: "zeta@example.com" },
  { kundeid: 7, name: "Eta GmbH", adresse: "Handelsweg 22, Stuttgart", kontaktdaten: "eta@example.com" },
  { kundeid: 8, name: "Theta Inc.", adresse: "Innovationspark 4, Düsseldorf", kontaktdaten: "theta@example.com" },
  { kundeid: 9, name: "Iota OHG", adresse: "Maschinenring 9, Bremen", kontaktdaten: "iota@example.com" },
  { kundeid: 10, name: "Kappa e.K.", adresse: "Lagerplatz 6, Dresden", kontaktdaten: "kappa@example.com" }
]);

// 2. Zahnradtypen einfügen (gekürzt)
db.zahnradtyp.insertMany([
  { zahnradtypid: 1, typbezeichnung: "Stirnrad", durchmesser: 25, vorgabeproduktionszeit: 500 },
  { zahnradtypid: 2, typbezeichnung: "Schrägverzahntes Zahnrad", durchmesser: 40, vorgabeproduktionszeit: 75 },
  { zahnradtypid: 3, typbezeichnung: "Kegelrad", durchmesser: 30, vorgabeproduktionszeit: 90 }
  // ... weitere Typen ...
]);

// 3. Maschinen einfügen (gekürzt)
db.maschine.insertMany([
  { maschineid: 1, hersteller: "Siemens", baujahr: 2015 },
  { maschineid: 2, hersteller: "Bosch", baujahr: 2016 },
  { maschineid: 3, hersteller: "GE", baujahr: 2014 }
  // ... weitere Maschinen ...
]);

// 4. Maschinenfähigkeiten einfügen (gekürzt)
db.maschinenfaehigkeit.insertMany([
  { maschineid: 1, zahnradtypid: 1 },
  { maschineid: 1, zahnradtypid: 2 },
  { maschineid: 2, zahnradtypid: 3 }
]);

// 5. Aufträge einfügen (100.008 Aufträge)
for (let i = 1; i <= 100008; i++) {
  db.auftrag.insertOne({
    auftragid: i,
    kundeid: Math.floor(Math.random() * 9) + 1,
    bestelldatum: new Date(2024, 3, 1 + Math.floor(Math.random() * 30)),
    geplantesfertigstellungsdatum: new Date(2024, 4, 1 + Math.floor(Math.random() * 30)),
    tatsaechlichesfertigstellungsdatum: null
  });
}

// 6. Auftragspositionen einfügen (100.000 Einträge)
for (let i = 1; i <= 100000; i++) {
  db.auftrag_position.insertOne({
    auftragpositionid: i,
    auftragid: Math.floor(Math.random() * 100008) + 1,
    zahnradtypid: Math.floor(Math.random() * 20) + 1,
    bestelltemenge: Math.floor(Math.random() * 100) + 1,
    geplanteproduktionsdauer: Math.floor(Math.random() * 150) + 50
  });
}

// 7. Produktion mit Ausschuss (100.000 Einträge)
for (let i = 1; i <= 100000; i++) {
  let start = new Date(2024, 4, 1, 8, 0, 0);
  let offset = Math.floor(Math.random() * 30 * 24 * 60);
  start.setMinutes(start.getMinutes() + offset);
  let end = new Date(start);
  end.setMinutes(end.getMinutes() + Math.floor(Math.random() * 55) + 5);
  db.produktion.insertOne({
    produktionid: i,
    auftragpositionid: Math.floor(Math.random() * 100000) + 1,
    maschineid: Math.floor(Math.random() * 10) + 1,
    startzeit: start,
    endzeit: end,
    ausschuss: Math.random() < 0.1
  });
}
