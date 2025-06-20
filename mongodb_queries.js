
// 1. Kundenname und Auftragsdatum
db.auftrag.aggregate([
  {
    $lookup: {
      from: "kunde",
      localField: "kundeid",
      foreignField: "kundeid",
      as: "kunde_info"
    }
  },
  { $unwind: "$kunde_info" },
  {
    $project: {
      _id: 0,
      kundenname: "$kunde_info.name",
      bestelldatum: 1
    }
  }
]);

// 2. Zahnräder in einem Auftrag
db.auftrag_position.aggregate([
  {
    $lookup: {
      from: "zahnradtyp",
      localField: "zahnradtypid",
      foreignField: "zahnradtypid",
      as: "zahnrad_info"
    }
  },
  { $unwind: "$zahnrad_info" },
  {
    $project: {
      _id: 0,
      auftragid: 1,
      typbezeichnung: "$zahnrad_info.typbezeichnung",
      bestelltemenge: 1
    }
  }
]);

// 3. Produktionsmaschine für ein Zahnrad
db.produktion.aggregate([
  {
    $lookup: {
      from: "maschine",
      localField: "maschineid",
      foreignField: "maschineid",
      as: "maschine_info"
    }
  },
  { $unwind: "$maschine_info" },
  {
    $lookup: {
      from: "auftrag_position",
      localField: "auftragpositionid",
      foreignField: "auftragpositionid",
      as: "auftrag_pos_info"
    }
  },
  { $unwind: "$auftrag_pos_info" },
  {
    $lookup: {
      from: "zahnradtyp",
      localField: "auftrag_pos_info.zahnradtypid",
      foreignField: "zahnradtypid",
      as: "zahnrad_info"
    }
  },
  { $unwind: "$zahnrad_info" },
  {
    $project: {
      _id: 0,
      produktionid: 1,
      hersteller: "$maschine_info.hersteller",
      typbezeichnung: "$zahnrad_info.typbezeichnung"
    }
  }
]);

// 4. Auslastung pro Maschine (Anzahl Produktionen pro Maschine)
db.maschine.aggregate([
  {
    $lookup: {
      from: "produktion",
      localField: "maschineid",
      foreignField: "maschineid",
      as: "produktionen"
    }
  },
  {
    $addFields: {
      produktionen_count: { $size: "$produktionen" }
    }
  },
  {
    $project: {
      _id: 0,
      maschineid: 1,
      hersteller: 1,
      produktionen: "$produktionen_count"
    }
  },
  { $sort: { produktionen: -1 } }
]);

// 5. Durchschnittliche Produktionsdauer pro ZahnradTyp (nur ohne Ausschuss)
db.produktion.aggregate([
  { $match: { ausschuss: false } },
  {
    $lookup: {
      from: "auftrag_position",
      localField: "auftragpositionid",
      foreignField: "auftragpositionid",
      as: "auftrag_pos_info"
    }
  },
  { $unwind: "$auftrag_pos_info" },
  {
    $lookup: {
      from: "zahnradtyp",
      localField: "auftrag_pos_info.zahnradtypid",
      foreignField: "zahnradtypid",
      as: "zahnrad_info"
    }
  },
  { $unwind: "$zahnrad_info" },
  {
    $addFields: {
      dauer_min: {
        $divide: [
          { $subtract: [ "$endzeit", "$startzeit" ] },
          1000 * 60
        ]
      }
    }
  },
  {
    $group: {
      _id: "$zahnrad_info.typbezeichnung",
      durchschnittszeit_min: { $avg: "$dauer_min" }
    }
  },
  {
    $project: {
      _id: 0,
      typbezeichnung: "$_id",
      durchschnittszeit_min: 1
    }
  }
]);
