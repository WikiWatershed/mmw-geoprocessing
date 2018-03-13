package org.wikiwatershed.mmw.geoprocessing

import java.util.concurrent.atomic.{LongAdder, DoubleAdder, DoubleAccumulator}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import collection.concurrent.TrieMap

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._

import geotrellis.spark._

trait Geoprocessing extends Utils {
  // TODO Remove after multiOperation is implemented
  val mocks: Map[String, Map[String, Double]] = Map (
    "nlcd_soils" -> Map (
      "List(71, 6)" -> 268,
      "List(81, 3)" -> 82961,
      "List(11, 2)" -> 4954,
      "List(21, 2)" -> 240211,
      "List(0, 6)" -> 49,
      "List(41, 7)" -> 159556,
      "List(90, 2)" -> 7606,
      "List(31, 1)" -> 15667,
      "List(43, -2147483648)" -> 4071,
      "List(52, 6)" -> 2030,
      "List(22, 6)" -> 4754,
      "List(23, 3)" -> 7465,
      "List(71, 7)" -> 2956,
      "List(11, 3)" -> 1229,
      "List(81, 7)" -> 32046,
      "List(0, 7)" -> 5,
      "List(95, 1)" -> 87,
      "List(82, 1)" -> 104583,
      "List(0, -2147483648)" -> 174,
      "List(43, 4)" -> 14639,
      "List(22, -2147483648)" -> 213390,
      "List(81, 4)" -> 151904,
      "List(41, 4)" -> 219019,
      "List(42, 2)" -> 16239,
      "List(90, 4)" -> 8094,
      "List(95, 6)" -> 1110,
      "List(11, 6)" -> 1966,
      "List(81, 6)" -> 21591,
      "List(23, 1)" -> 7003,
      "List(21, 4)" -> 89381,
      "List(23, -2147483648)" -> 153940,
      "List(82, 6)" -> 8865,
      "List(31, 2)" -> 1930,
      "List(41, 3)" -> 169841,
      "List(43, 7)" -> 5932,
      "List(52, 4)" -> 28662,
      "List(42, 3)" -> 4711,
      "List(95, 7)" -> 574,
      "List(90, 7)" -> 22064,
      "List(22, 7)" -> 10416,
      "List(21, 7)" -> 46919,
      "List(31, 4)" -> 1506,
      "List(11, 1)" -> 4231,
      "List(0, 1)" -> 149,
      "List(22, 2)" -> 87254,
      "List(43, 1)" -> 19955,
      "List(82, 7)" -> 31265,
      "List(41, 2)" -> 584572,
      "List(52, 3)" -> 23913,
      "List(95, -2147483648)" -> 1114,
      "List(31, 3)" -> 1396,
      "List(43, 6)" -> 2305,
      "List(71, 1)" -> 9131,
      "List(11, -2147483648)" -> 44525,
      "List(90, -2147483648)" -> 7753,
      "List(82, -2147483648)" -> 6652,
      "List(95, 4)" -> 930,
      "List(42, 1)" -> 11380,
      "List(90, 6)" -> 16551,
      "List(81, 1)" -> 115211,
      "List(24, 1)" -> 2570,
      "List(23, 7)" -> 2845,
      "List(21, 6)" -> 17228,
      "List(0, 2)" -> 437,
      "List(52, 2)" -> 62373,
      "List(81, -2147483648)" -> 13943,
      "List(41, 1)" -> 697001,
      "List(71, 2)" -> 9940,
      "List(24, 3)" -> 2169,
      "List(24, -2147483648)" -> 86936,
      "List(21, 1)" -> 94657,
      "List(22, 1)" -> 19717,
      "List(90, 1)" -> 2427,
      "List(31, 6)" -> 138,
      "List(23, 6)" -> 1429,
      "List(24, 6)" -> 447,
      "List(43, 3)" -> 7030,
      "List(0, 3)" -> 77,
      "List(82, 4)" -> 79426,
      "List(52, 1)" -> 50503,
      "List(41, -2147483648)" -> 61560,
      "List(22, 3)" -> 27001,
      "List(42, 6)" -> 2965,
      "List(71, 3)" -> 2788,
      "List(81, 2)" -> 470329,
      "List(0, 4)" -> 37,
      "List(71, -2147483648)" -> 1565,
      "List(95, 2)" -> 554,
      "List(82, 2)" -> 356754,
      "List(42, -2147483648)" -> 1821,
      "List(24, 7)" -> 485,
      "List(11, 4)" -> 1846,
      "List(31, 7)" -> 796,
      "List(31, -2147483648)" -> 9839,
      "List(52, -2147483648)" -> 8573,
      "List(22, 4)" -> 30903,
      "List(42, 7)" -> 3837,
      "List(23, 2)" -> 30235,
      "List(24, 2)" -> 9055,
      "List(71, 4)" -> 5299,
      "List(21, 3)" -> 87922,
      "List(95, 3)" -> 258,
      "List(90, 3)" -> 8340,
      "List(82, 3)" -> 63108,
      "List(24, 4)" -> 1945,
      "List(23, 4)" -> 7611,
      "List(43, 2)" -> 17857,
      "List(21, -2147483648)" -> 218609,
      "List(41, 6)" -> 39058,
      "List(52, 7)" -> 28038,
      "List(42, 4)" -> 13093,
      "List(11, 7)" -> 429
    ),
    "nlcd_streams" -> Map (
      "List(90)" -> 14005,
      "List(95)" -> 677,
      "List(21)" -> 11553,
      "List(52)" -> 2874,
      "List(31)" -> 217,
      "List(11)" -> 8095,
      "List(71)" -> 150,
      "List(0)" -> 13,
      "List(41)" -> 33341,
      "List(24)" -> 364,
      "List(81)" -> 10301,
      "List(42)" -> 2149,
      "List(43)" -> 2387,
      "List(22)" -> 3252,
      "List(23)" -> 1326,
      "List(82)" -> 4736
    ),
    "gwn" -> Map (
      "List(-256)" -> 80230,
      "List(0)" -> 1277530,
      "List(1)" -> 786036,
      "List(10)" -> 24913,
      "List(11)" -> 24840,
      "List(12)" -> 43734,
      "List(13)" -> 63094,
      "List(14)" -> 5530,
      "List(15)" -> 5454,
      "List(16)" -> 3503,
      "List(17)" -> 5578,
      "List(18)" -> 1581,
      "List(2)" -> 780608,
      "List(20)" -> 37,
      "List(22)" -> 1089,
      "List(23)" -> 1586,
      "List(27)" -> 14,
      "List(28)" -> 119,
      "List(3)" -> 695603,
      "List(31)" -> 588,
      "List(34)" -> 423,
      "List(35)" -> 2175,
      "List(4)" -> 504678,
      "List(46)" -> 224,
      "List(5)" -> 372267,
      "List(6)" -> 376547,
      "List(7)" -> 281170,
      "List(8)" -> 111029,
      "List(9)" -> 52278
    ),
    "avg_awc" -> Map ("List(0)" -> 8.8224446790259),
    "nlcd_slope" -> Map (
      "List(0, 0)" -> 35,
      "List(0, 1)" -> 123,
      "List(0, 10)" -> 66,
      "List(0, 12)" -> 47,
      "List(0, 14)" -> 35,
      "List(0, 15)" -> 20,
      "List(0, 17)" -> 25,
      "List(0, 19)" -> 22,
      "List(0, 21)" -> 15,
      "List(0, 23)" -> 10,
      "List(0, 24)" -> 4,
      "List(0, 26)" -> 2,
      "List(0, 28)" -> 7,
      "List(0, 3)" -> 144,
      "List(0, 30)" -> 2,
      "List(0, 32)" -> 3,
      "List(0, 34)" -> 2,
      "List(0, 36)" -> 3,
      "List(0, 38)" -> 3,
      "List(0, 40)" -> 3,
      "List(0, 42)" -> 2,
      "List(0, 44)" -> 2,
      "List(0, 46)" -> 4,
      "List(0, 5)" -> 126,
      "List(0, 6)" -> 131,
      "List(0, 8)" -> 92,
      "List(11, 0)" -> 27052,
      "List(11, 1)" -> 8471,
      "List(11, 10)" -> 1614,
      "List(11, 100)" -> 22,
      "List(11, 103)" -> 9,
      "List(11, 107)" -> 12,
      "List(11, 111)" -> 4,
      "List(11, 115)" -> 6,
      "List(11, 119)" -> 3,
      "List(11, 12)" -> 1192,
      "List(11, 123)" -> 2,
      "List(11, 127)" -> 5,
      "List(11, 137)" -> 4,
      "List(11, 14)" -> 910,
      "List(11, 142)" -> 3,
      "List(11, 148)" -> 2,
      "List(11, 15)" -> 755,
      "List(11, 153)" -> 3,
      "List(11, 160)" -> 3,
      "List(11, 166)" -> 4,
      "List(11, 17)" -> 609,
      "List(11, 19)" -> 523,
      "List(11, 21)" -> 454,
      "List(11, 23)" -> 393,
      "List(11, 24)" -> 334,
      "List(11, 26)" -> 285,
      "List(11, 28)" -> 261,
      "List(11, 3)" -> 5326,
      "List(11, 30)" -> 243,
      "List(11, 32)" -> 200,
      "List(11, 34)" -> 178,
      "List(11, 36)" -> 142,
      "List(11, 38)" -> 145,
      "List(11, 40)" -> 121,
      "List(11, 42)" -> 116,
      "List(11, 44)" -> 101,
      "List(11, 46)" -> 109,
      "List(11, 48)" -> 96,
      "List(11, 5)" -> 3873,
      "List(11, 50)" -> 81,
      "List(11, 53)" -> 83,
      "List(11, 55)" -> 87,
      "List(11, 57)" -> 68,
      "List(11, 6)" -> 2627,
      "List(11, 60)" -> 68,
      "List(11, 62)" -> 55,
      "List(11, 64)" -> 62,
      "List(11, 67)" -> 46,
      "List(11, 70)" -> 54,
      "List(11, 72)" -> 43,
      "List(11, 75)" -> 47,
      "List(11, 78)" -> 22,
      "List(11, 8)" -> 2122,
      "List(11, 80)" -> 27,
      "List(11, 83)" -> 27,
      "List(11, 86)" -> 18,
      "List(11, 90)" -> 23,
      "List(11, 93)" -> 21,
      "List(11, 96)" -> 14,
      "List(21, 0)" -> 59333,
      "List(21, 1)" -> 152659,
      "List(21, 10)" -> 43383,
      "List(21, 100)" -> 1,
      "List(21, 103)" -> 1,
      "List(21, 107)" -> 1,
      "List(21, 111)" -> 1,
      "List(21, 119)" -> 1,
      "List(21, 12)" -> 30867,
      "List(21, 123)" -> 3,
      "List(21, 132)" -> 2,
      "List(21, 14)" -> 22126,
      "List(21, 148)" -> 1,
      "List(21, 15)" -> 16554,
      "List(21, 17)" -> 12237,
      "List(21, 19)" -> 9567,
      "List(21, 21)" -> 6774,
      "List(21, 23)" -> 5298,
      "List(21, 24)" -> 4101,
      "List(21, 26)" -> 3236,
      "List(21, 28)" -> 2536,
      "List(21, 3)" -> 145563,
      "List(21, 30)" -> 2037,
      "List(21, 32)" -> 1655,
      "List(21, 34)" -> 1280,
      "List(21, 36)" -> 998,
      "List(21, 38)" -> 819,
      "List(21, 40)" -> 641,
      "List(21, 42)" -> 531,
      "List(21, 44)" -> 394,
      "List(21, 46)" -> 309,
      "List(21, 48)" -> 251,
      "List(21, 5)" -> 121976,
      "List(21, 50)" -> 202,
      "List(21, 53)" -> 194,
      "List(21, 55)" -> 136,
      "List(21, 57)" -> 105,
      "List(21, 6)" -> 85060,
      "List(21, 60)" -> 92,
      "List(21, 62)" -> 75,
      "List(21, 64)" -> 45,
      "List(21, 67)" -> 40,
      "List(21, 70)" -> 31,
      "List(21, 72)" -> 21,
      "List(21, 75)" -> 12,
      "List(21, 78)" -> 14,
      "List(21, 8)" -> 63736,
      "List(21, 80)" -> 6,
      "List(21, 83)" -> 11,
      "List(21, 86)" -> 5,
      "List(21, 90)" -> 5,
      "List(21, 96)" -> 1,
      "List(22, 0)" -> 42434,
      "List(22, 1)" -> 101217,
      "List(22, 10)" -> 14765,
      "List(22, 100)" -> 1,
      "List(22, 103)" -> 1,
      "List(22, 107)" -> 3,
      "List(22, 111)" -> 4,
      "List(22, 115)" -> 1,
      "List(22, 12)" -> 9377,
      "List(22, 127)" -> 1,
      "List(22, 14)" -> 6092,
      "List(22, 142)" -> 1,
      "List(22, 15)" -> 4205,
      "List(22, 17)" -> 2801,
      "List(22, 19)" -> 2015,
      "List(22, 21)" -> 1351,
      "List(22, 23)" -> 1000,
      "List(22, 24)" -> 677,
      "List(22, 26)" -> 514,
      "List(22, 28)" -> 388,
      "List(22, 3)" -> 83599,
      "List(22, 30)" -> 299,
      "List(22, 32)" -> 253,
      "List(22, 34)" -> 162,
      "List(22, 36)" -> 170,
      "List(22, 38)" -> 112,
      "List(22, 40)" -> 66,
      "List(22, 42)" -> 78,
      "List(22, 44)" -> 73,
      "List(22, 46)" -> 50,
      "List(22, 48)" -> 44,
      "List(22, 5)" -> 60725,
      "List(22, 50)" -> 33,
      "List(22, 53)" -> 38,
      "List(22, 55)" -> 22,
      "List(22, 57)" -> 24,
      "List(22, 6)" -> 36559,
      "List(22, 60)" -> 21,
      "List(22, 62)" -> 13,
      "List(22, 64)" -> 11,
      "List(22, 67)" -> 10,
      "List(22, 70)" -> 4,
      "List(22, 72)" -> 9,
      "List(22, 75)" -> 4,
      "List(22, 78)" -> 5,
      "List(22, 8)" -> 24185,
      "List(22, 80)" -> 6,
      "List(22, 83)" -> 4,
      "List(22, 86)" -> 3,
      "List(22, 90)" -> 2,
      "List(22, 93)" -> 3,
      "List(23, 0)" -> 36562,
      "List(23, 1)" -> 61184,
      "List(23, 10)" -> 5955,
      "List(23, 100)" -> 2,
      "List(23, 103)" -> 3,
      "List(23, 111)" -> 1,
      "List(23, 115)" -> 1,
      "List(23, 12)" -> 3647,
      "List(23, 123)" -> 1,
      "List(23, 14)" -> 2214,
      "List(23, 15)" -> 1427,
      "List(23, 17)" -> 1064,
      "List(23, 19)" -> 734,
      "List(23, 21)" -> 571,
      "List(23, 23)" -> 387,
      "List(23, 24)" -> 330,
      "List(23, 26)" -> 203,
      "List(23, 28)" -> 148,
      "List(23, 3)" -> 41867,
      "List(23, 30)" -> 132,
      "List(23, 32)" -> 100,
      "List(23, 34)" -> 91,
      "List(23, 36)" -> 83,
      "List(23, 38)" -> 61,
      "List(23, 40)" -> 48,
      "List(23, 42)" -> 36,
      "List(23, 44)" -> 40,
      "List(23, 46)" -> 31,
      "List(23, 48)" -> 23,
      "List(23, 5)" -> 27713,
      "List(23, 50)" -> 31,
      "List(23, 53)" -> 20,
      "List(23, 55)" -> 19,
      "List(23, 57)" -> 11,
      "List(23, 6)" -> 15734,
      "List(23, 60)" -> 23,
      "List(23, 62)" -> 15,
      "List(23, 64)" -> 24,
      "List(23, 67)" -> 13,
      "List(23, 70)" -> 13,
      "List(23, 72)" -> 17,
      "List(23, 75)" -> 14,
      "List(23, 78)" -> 9,
      "List(23, 8)" -> 9897,
      "List(23, 80)" -> 13,
      "List(23, 83)" -> 7,
      "List(23, 86)" -> 4,
      "List(23, 90)" -> 2,
      "List(23, 93)" -> 1,
      "List(23, 96)" -> 2,
      "List(24, 0)" -> 34720,
      "List(24, 1)" -> 34697,
      "List(24, 10)" -> 1260,
      "List(24, 107)" -> 1,
      "List(24, 12)" -> 727,
      "List(24, 14)" -> 405,
      "List(24, 15)" -> 275,
      "List(24, 17)" -> 184,
      "List(24, 19)" -> 139,
      "List(24, 21)" -> 78,
      "List(24, 23)" -> 58,
      "List(24, 24)" -> 50,
      "List(24, 26)" -> 38,
      "List(24, 28)" -> 29,
      "List(24, 3)" -> 15951,
      "List(24, 30)" -> 36,
      "List(24, 32)" -> 22,
      "List(24, 34)" -> 27,
      "List(24, 36)" -> 20,
      "List(24, 38)" -> 22,
      "List(24, 40)" -> 20,
      "List(24, 42)" -> 23,
      "List(24, 44)" -> 16,
      "List(24, 46)" -> 22,
      "List(24, 48)" -> 20,
      "List(24, 5)" -> 8115,
      "List(24, 50)" -> 17,
      "List(24, 53)" -> 17,
      "List(24, 55)" -> 13,
      "List(24, 57)" -> 8,
      "List(24, 6)" -> 4111,
      "List(24, 60)" -> 11,
      "List(24, 62)" -> 20,
      "List(24, 64)" -> 16,
      "List(24, 67)" -> 15,
      "List(24, 70)" -> 9,
      "List(24, 72)" -> 11,
      "List(24, 75)" -> 11,
      "List(24, 78)" -> 4,
      "List(24, 8)" -> 2370,
      "List(24, 80)" -> 8,
      "List(24, 83)" -> 1,
      "List(24, 86)" -> 4,
      "List(24, 90)" -> 4,
      "List(24, 96)" -> 2,
      "List(31, 0)" -> 1894,
      "List(31, 1)" -> 3413,
      "List(31, 10)" -> 1681,
      "List(31, 100)" -> 17,
      "List(31, 103)" -> 18,
      "List(31, 107)" -> 10,
      "List(31, 111)" -> 7,
      "List(31, 115)" -> 6,
      "List(31, 119)" -> 11,
      "List(31, 12)" -> 1517,
      "List(31, 123)" -> 11,
      "List(31, 127)" -> 8,
      "List(31, 132)" -> 5,
      "List(31, 137)" -> 4,
      "List(31, 14)" -> 1342,
      "List(31, 142)" -> 4,
      "List(31, 148)" -> 4,
      "List(31, 15)" -> 1160,
      "List(31, 153)" -> 3,
      "List(31, 160)" -> 2,
      "List(31, 17)" -> 1121,
      "List(31, 173)" -> 1,
      "List(31, 19)" -> 1087,
      "List(31, 21)" -> 878,
      "List(31, 23)" -> 778,
      "List(31, 24)" -> 677,
      "List(31, 26)" -> 603,
      "List(31, 28)" -> 521,
      "List(31, 3)" -> 3000,
      "List(31, 30)" -> 454,
      "List(31, 32)" -> 411,
      "List(31, 34)" -> 372,
      "List(31, 36)" -> 328,
      "List(31, 38)" -> 329,
      "List(31, 40)" -> 290,
      "List(31, 42)" -> 292,
      "List(31, 44)" -> 256,
      "List(31, 46)" -> 235,
      "List(31, 48)" -> 264,
      "List(31, 5)" -> 2659,
      "List(31, 50)" -> 181,
      "List(31, 53)" -> 177,
      "List(31, 55)" -> 156,
      "List(31, 57)" -> 152,
      "List(31, 6)" -> 2089,
      "List(31, 60)" -> 131,
      "List(31, 62)" -> 117,
      "List(31, 64)" -> 109,
      "List(31, 67)" -> 92,
      "List(31, 70)" -> 71,
      "List(31, 72)" -> 63,
      "List(31, 75)" -> 58,
      "List(31, 78)" -> 44,
      "List(31, 8)" -> 1943,
      "List(31, 80)" -> 46,
      "List(31, 83)" -> 40,
      "List(31, 86)" -> 46,
      "List(31, 90)" -> 44,
      "List(31, 93)" -> 26,
      "List(31, 96)" -> 14,
      "List(41, 0)" -> 50473,
      "List(41, 1)" -> 127164,
      "List(41, 10)" -> 150653,
      "List(41, 100)" -> 7,
      "List(41, 103)" -> 4,
      "List(41, 107)" -> 3,
      "List(41, 111)" -> 3,
      "List(41, 115)" -> 1,
      "List(41, 119)" -> 4,
      "List(41, 12)" -> 136713,
      "List(41, 123)" -> 3,
      "List(41, 127)" -> 3,
      "List(41, 132)" -> 2,
      "List(41, 137)" -> 3,
      "List(41, 14)" -> 121800,
      "List(41, 142)" -> 2,
      "List(41, 15)" -> 109576,
      "List(41, 17)" -> 91838,
      "List(41, 19)" -> 80243,
      "List(41, 21)" -> 65247,
      "List(41, 23)" -> 55701,
      "List(41, 24)" -> 46707,
      "List(41, 26)" -> 39174,
      "List(41, 28)" -> 33207,
      "List(41, 3)" -> 154174,
      "List(41, 30)" -> 28684,
      "List(41, 32)" -> 23709,
      "List(41, 34)" -> 19611,
      "List(41, 36)" -> 16668,
      "List(41, 38)" -> 13970,
      "List(41, 40)" -> 11403,
      "List(41, 42)" -> 9520,
      "List(41, 44)" -> 7829,
      "List(41, 46)" -> 6422,
      "List(41, 48)" -> 5145,
      "List(41, 5)" -> 175005,
      "List(41, 50)" -> 4281,
      "List(41, 53)" -> 3563,
      "List(41, 55)" -> 2618,
      "List(41, 57)" -> 2129,
      "List(41, 6)" -> 165001,
      "List(41, 60)" -> 1608,
      "List(41, 62)" -> 1234,
      "List(41, 64)" -> 775,
      "List(41, 67)" -> 601,
      "List(41, 70)" -> 402,
      "List(41, 72)" -> 241,
      "List(41, 75)" -> 153,
      "List(41, 78)" -> 95,
      "List(41, 8)" -> 167101,
      "List(41, 80)" -> 46,
      "List(41, 83)" -> 41,
      "List(41, 86)" -> 16,
      "List(41, 90)" -> 12,
      "List(41, 93)" -> 10,
      "List(41, 96)" -> 9,
      "List(42, 0)" -> 2404,
      "List(42, 1)" -> 6589,
      "List(42, 10)" -> 3415,
      "List(42, 12)" -> 2875,
      "List(42, 14)" -> 2408,
      "List(42, 15)" -> 2128,
      "List(42, 17)" -> 1831,
      "List(42, 19)" -> 1637,
      "List(42, 21)" -> 1318,
      "List(42, 23)" -> 1139,
      "List(42, 24)" -> 961,
      "List(42, 26)" -> 806,
      "List(42, 28)" -> 667,
      "List(42, 3)" -> 7175,
      "List(42, 30)" -> 576,
      "List(42, 32)" -> 480,
      "List(42, 34)" -> 354,
      "List(42, 36)" -> 291,
      "List(42, 38)" -> 201,
      "List(42, 40)" -> 171,
      "List(42, 42)" -> 136,
      "List(42, 44)" -> 120,
      "List(42, 46)" -> 80,
      "List(42, 48)" -> 71,
      "List(42, 5)" -> 6732,
      "List(42, 50)" -> 55,
      "List(42, 53)" -> 43,
      "List(42, 55)" -> 44,
      "List(42, 57)" -> 31,
      "List(42, 6)" -> 4985,
      "List(42, 60)" -> 21,
      "List(42, 62)" -> 18,
      "List(42, 64)" -> 11,
      "List(42, 67)" -> 14,
      "List(42, 70)" -> 9,
      "List(42, 72)" -> 8,
      "List(42, 75)" -> 3,
      "List(42, 78)" -> 3,
      "List(42, 8)" -> 4234,
      "List(42, 93)" -> 2,
      "List(43, 0)" -> 2344,
      "List(43, 1)" -> 6399,
      "List(43, 10)" -> 4872,
      "List(43, 103)" -> 2,
      "List(43, 12)" -> 4116,
      "List(43, 14)" -> 3416,
      "List(43, 15)" -> 2996,
      "List(43, 17)" -> 2534,
      "List(43, 19)" -> 2258,
      "List(43, 21)" -> 1969,
      "List(43, 23)" -> 1661,
      "List(43, 24)" -> 1560,
      "List(43, 26)" -> 1380,
      "List(43, 28)" -> 1123,
      "List(43, 3)" -> 8028,
      "List(43, 30)" -> 1123,
      "List(43, 32)" -> 882,
      "List(43, 34)" -> 749,
      "List(43, 36)" -> 682,
      "List(43, 38)" -> 559,
      "List(43, 40)" -> 484,
      "List(43, 42)" -> 375,
      "List(43, 44)" -> 299,
      "List(43, 46)" -> 242,
      "List(43, 48)" -> 241,
      "List(43, 5)" -> 7877,
      "List(43, 50)" -> 190,
      "List(43, 53)" -> 191,
      "List(43, 55)" -> 119,
      "List(43, 57)" -> 99,
      "List(43, 6)" -> 6533,
      "List(43, 60)" -> 89,
      "List(43, 62)" -> 58,
      "List(43, 64)" -> 51,
      "List(43, 67)" -> 47,
      "List(43, 70)" -> 24,
      "List(43, 72)" -> 25,
      "List(43, 75)" -> 26,
      "List(43, 78)" -> 17,
      "List(43, 8)" -> 6129,
      "List(43, 80)" -> 10,
      "List(43, 83)" -> 6,
      "List(43, 86)" -> 3,
      "List(43, 93)" -> 1,
      "List(52, 0)" -> 9495,
      "List(52, 1)" -> 26629,
      "List(52, 10)" -> 15505,
      "List(52, 12)" -> 11994,
      "List(52, 14)" -> 8766,
      "List(52, 15)" -> 6592,
      "List(52, 17)" -> 4787,
      "List(52, 19)" -> 3504,
      "List(52, 21)" -> 2405,
      "List(52, 23)" -> 1704,
      "List(52, 24)" -> 1274,
      "List(52, 26)" -> 888,
      "List(52, 28)" -> 670,
      "List(52, 3)" -> 30732,
      "List(52, 30)" -> 516,
      "List(52, 32)" -> 327,
      "List(52, 34)" -> 236,
      "List(52, 36)" -> 171,
      "List(52, 38)" -> 168,
      "List(52, 40)" -> 112,
      "List(52, 42)" -> 91,
      "List(52, 44)" -> 76,
      "List(52, 46)" -> 65,
      "List(52, 48)" -> 47,
      "List(52, 5)" -> 30975,
      "List(52, 50)" -> 52,
      "List(52, 53)" -> 35,
      "List(52, 55)" -> 24,
      "List(52, 57)" -> 31,
      "List(52, 6)" -> 25172,
      "List(52, 60)" -> 15,
      "List(52, 62)" -> 7,
      "List(52, 64)" -> 15,
      "List(52, 67)" -> 9,
      "List(52, 70)" -> 3,
      "List(52, 72)" -> 4,
      "List(52, 75)" -> 1,
      "List(52, 78)" -> 2,
      "List(52, 8)" -> 20991,
      "List(52, 80)" -> 1,
      "List(52, 83)" -> 1,
      "List(71, 0)" -> 1960,
      "List(71, 1)" -> 4866,
      "List(71, 10)" -> 2042,
      "List(71, 12)" -> 1494,
      "List(71, 14)" -> 1245,
      "List(71, 15)" -> 1016,
      "List(71, 17)" -> 814,
      "List(71, 19)" -> 639,
      "List(71, 21)" -> 480,
      "List(71, 23)" -> 459,
      "List(71, 24)" -> 350,
      "List(71, 26)" -> 234,
      "List(71, 28)" -> 175,
      "List(71, 3)" -> 5037,
      "List(71, 30)" -> 125,
      "List(71, 32)" -> 84,
      "List(71, 34)" -> 58,
      "List(71, 36)" -> 70,
      "List(71, 38)" -> 34,
      "List(71, 40)" -> 36,
      "List(71, 42)" -> 33,
      "List(71, 44)" -> 33,
      "List(71, 46)" -> 12,
      "List(71, 48)" -> 7,
      "List(71, 5)" -> 4265,
      "List(71, 50)" -> 9,
      "List(71, 53)" -> 5,
      "List(71, 55)" -> 4,
      "List(71, 57)" -> 7,
      "List(71, 6)" -> 3447,
      "List(71, 60)" -> 6,
      "List(71, 62)" -> 4,
      "List(71, 64)" -> 6,
      "List(71, 70)" -> 3,
      "List(71, 72)" -> 1,
      "List(71, 78)" -> 1,
      "List(71, 8)" -> 2886,
      "List(81, 0)" -> 53296,
      "List(81, 1)" -> 139765,
      "List(81, 10)" -> 64754,
      "List(81, 103)" -> 1,
      "List(81, 119)" -> 1,
      "List(81, 12)" -> 48112,
      "List(81, 123)" -> 1,
      "List(81, 14)" -> 34735,
      "List(81, 15)" -> 25225,
      "List(81, 17)" -> 17333,
      "List(81, 19)" -> 11860,
      "List(81, 21)" -> 7576,
      "List(81, 23)" -> 4880,
      "List(81, 24)" -> 3147,
      "List(81, 26)" -> 1893,
      "List(81, 28)" -> 1214,
      "List(81, 3)" -> 144506,
      "List(81, 30)" -> 842,
      "List(81, 32)" -> 489,
      "List(81, 34)" -> 282,
      "List(81, 36)" -> 251,
      "List(81, 38)" -> 136,
      "List(81, 40)" -> 111,
      "List(81, 42)" -> 74,
      "List(81, 44)" -> 68,
      "List(81, 46)" -> 47,
      "List(81, 48)" -> 22,
      "List(81, 5)" -> 136552,
      "List(81, 50)" -> 22,
      "List(81, 53)" -> 14,
      "List(81, 55)" -> 5,
      "List(81, 57)" -> 9,
      "List(81, 6)" -> 104712,
      "List(81, 60)" -> 7,
      "List(81, 62)" -> 7,
      "List(81, 67)" -> 1,
      "List(81, 70)" -> 1,
      "List(81, 72)" -> 1,
      "List(81, 78)" -> 1,
      "List(81, 8)" -> 86028,
      "List(81, 80)" -> 1,
      "List(81, 86)" -> 3,
      "List(82, 0)" -> 68162,
      "List(82, 1)" -> 150234,
      "List(82, 10)" -> 31118,
      "List(82, 107)" -> 1,
      "List(82, 12)" -> 20378,
      "List(82, 132)" -> 1,
      "List(82, 14)" -> 13127,
      "List(82, 15)" -> 9115,
      "List(82, 153)" -> 1,
      "List(82, 17)" -> 5726,
      "List(82, 19)" -> 3478,
      "List(82, 21)" -> 2092,
      "List(82, 23)" -> 1324,
      "List(82, 24)" -> 779,
      "List(82, 26)" -> 441,
      "List(82, 28)" -> 281,
      "List(82, 3)" -> 127994,
      "List(82, 30)" -> 168,
      "List(82, 32)" -> 112,
      "List(82, 34)" -> 85,
      "List(82, 36)" -> 57,
      "List(82, 38)" -> 32,
      "List(82, 40)" -> 49,
      "List(82, 42)" -> 33,
      "List(82, 44)" -> 24,
      "List(82, 46)" -> 16,
      "List(82, 48)" -> 18,
      "List(82, 5)" -> 100586,
      "List(82, 50)" -> 13,
      "List(82, 53)" -> 10,
      "List(82, 55)" -> 10,
      "List(82, 57)" -> 12,
      "List(82, 6)" -> 67283,
      "List(82, 60)" -> 3,
      "List(82, 62)" -> 8,
      "List(82, 64)" -> 2,
      "List(82, 67)" -> 2,
      "List(82, 70)" -> 1,
      "List(82, 72)" -> 3,
      "List(82, 75)" -> 1,
      "List(82, 78)" -> 3,
      "List(82, 8)" -> 47868,
      "List(82, 80)" -> 2,
      "List(90, 0)" -> 25099,
      "List(90, 1)" -> 21946,
      "List(90, 10)" -> 1134,
      "List(90, 12)" -> 804,
      "List(90, 14)" -> 525,
      "List(90, 15)" -> 345,
      "List(90, 17)" -> 263,
      "List(90, 19)" -> 171,
      "List(90, 21)" -> 136,
      "List(90, 23)" -> 116,
      "List(90, 24)" -> 73,
      "List(90, 26)" -> 53,
      "List(90, 28)" -> 46,
      "List(90, 3)" -> 10030,
      "List(90, 30)" -> 43,
      "List(90, 32)" -> 35,
      "List(90, 34)" -> 26,
      "List(90, 36)" -> 26,
      "List(90, 38)" -> 26,
      "List(90, 40)" -> 22,
      "List(90, 42)" -> 19,
      "List(90, 44)" -> 14,
      "List(90, 46)" -> 10,
      "List(90, 48)" -> 6,
      "List(90, 5)" -> 6302,
      "List(90, 50)" -> 11,
      "List(90, 53)" -> 11,
      "List(90, 55)" -> 5,
      "List(90, 57)" -> 3,
      "List(90, 6)" -> 3490,
      "List(90, 60)" -> 8,
      "List(90, 62)" -> 8,
      "List(90, 64)" -> 6,
      "List(90, 67)" -> 5,
      "List(90, 70)" -> 9,
      "List(90, 72)" -> 3,
      "List(90, 75)" -> 6,
      "List(90, 78)" -> 7,
      "List(90, 8)" -> 1980,
      "List(90, 80)" -> 4,
      "List(90, 83)" -> 5,
      "List(90, 86)" -> 1,
      "List(90, 90)" -> 2,
      "List(90, 93)" -> 1,
      "List(95, 0)" -> 1502,
      "List(95, 1)" -> 1133,
      "List(95, 10)" -> 114,
      "List(95, 12)" -> 89,
      "List(95, 14)" -> 77,
      "List(95, 15)" -> 42,
      "List(95, 17)" -> 29,
      "List(95, 19)" -> 32,
      "List(95, 21)" -> 23,
      "List(95, 23)" -> 17,
      "List(95, 24)" -> 8,
      "List(95, 26)" -> 12,
      "List(95, 28)" -> 9,
      "List(95, 3)" -> 597,
      "List(95, 30)" -> 7,
      "List(95, 32)" -> 5,
      "List(95, 34)" -> 4,
      "List(95, 36)" -> 4,
      "List(95, 38)" -> 6,
      "List(95, 40)" -> 3,
      "List(95, 42)" -> 2,
      "List(95, 44)" -> 1,
      "List(95, 46)" -> 1,
      "List(95, 48)" -> 4,
      "List(95, 5)" -> 430,
      "List(95, 6)" -> 280,
      "List(95, 8)" -> 196
    ),
    "slope" -> Map ("List(0)" -> 50.38815789473684),
    "nlcd_kfactor" -> Map (
      "List(0)" -> 0.20602414696397067,
      "List(11)" -> 0.05508165069169296,
      "List(21)" -> 0.21868470419874417,
      "List(22)" -> 0.16695226874641336,
      "List(23)" -> 0.112148292194999,
      "List(24)" -> 0.06672464966401591,
      "List(31)" -> 0.07092705045562692,
      "List(41)" -> 0.18912035761808532,
      "List(42)" -> 0.206511688111982,
      "List(43)" -> 0.19906280961858158,
      "List(52)" -> 0.2698116178955606,
      "List(71)" -> 0.22942145138570946,
      "List(81)" -> 0.23793733590310687,
      "List(82)" -> 0.2559927368472959,
      "List(90)" -> 0.31187355224088653,
      "List(95)" -> 0.24373768152729858
    )
  )

  def getMultiOperations(input: MultiInput): Map[String, Map[String, Map[String, Double]]] =
    // TODO Replace mock response with actual code
    input.shapes.map(huc => huc.id -> input.operations.map(op =>
      op.label -> mocks(op.label)
    ).toMap).toMap

  /**
    * For an InputData object, return a histogram of raster grouped count results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  def getRasterGroupedCount(input: InputData): Future[ResultInt] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { layers =>
      ResultInt(rasterGroupedCount(layers, aoi, opts))
    }
  }

  /**
    * For an InputData object, return a histogram of raster grouped average
    * results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  @throws(classOf[MissingTargetRasterException])
  def getRasterGroupedAverage(input: InputData): Future[ResultDouble] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val targetLayer = input.targetRaster match {
      case Some(targetRaster) =>
        cropSingleRasterToAOI(targetRaster, input.zoom, aoi)
      case None => throw new MissingTargetRasterException
    }
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { rasterLayers =>
      val average =
        if (rasterLayers.isEmpty) rasterAverage(targetLayer, aoi, opts)
        else rasterGroupedAverage(rasterLayers, targetLayer, aoi, opts)

      ResultDouble(average)
    }
  }

  /**
    * For an InputData object, return a histogram of raster lines join results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  @throws(classOf[MissingVectorException])
  @throws(classOf[MissingVectorCRSException])
  def getRasterLinesJoin(input: InputData): Future[ResultInt] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val lines = input.vector match {
      case Some(vector) =>
        input.vectorCRS match {
          case Some(crs) =>
            cropLinesToAOI(
              createMultiLineFromInput(vector, crs, input.rasterCRS), aoi)
          case None => throw new MissingVectorCRSException
        }
      case None => throw new MissingVectorException
    }

    futureLayers.map { rasterLayers =>
      ResultInt(rasterLinesJoin(rasterLayers, lines))
    }
  }


  /**
    * For an InputData object, returns a sequence of maps of min, avg, and max
    * values for each raster, in the order of the input rasters
    *
    * @param   input  The InputData
    * @return         Seq of map of min, avg, and max values
    */
  def getRasterSummary(input: InputData): Future[ResultSummary] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { layers =>
      ResultSummary(rasterSummary(layers, aoi, opts))
    }
  }

  private case class TilePixel(key: SpatialKey, col: Int, row: Int)

  /**
    * Given a collection of rasterLayers & a collection of lines, return the
    * values intersected by the rasterized lines.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   lines         A sequence of MultiLines
    * @return                A map of pixel counts
    */
  private def rasterLinesJoin(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    lines: Seq[MultiLine]
  ): Map[String, Int] = {
    val metadata = rasterLayers.head.metadata
    val pixelGroups: TrieMap[(List[Int], TilePixel), Int] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent = metadata.mapTransform(key)
        val re = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        lines.par.foreach({ multiLine =>
          Rasterizer.foreachCellByMultiLineString(multiLine, re) { case (col, row) =>
            val pixelGroup: (List[Int], TilePixel) =
              (tiles.map(_.get(col, row)).toList, TilePixel(key, col, row))
            pixelGroups.getOrElseUpdate(pixelGroup, 1)
          }
        })
      })

    pixelGroups
      .groupBy(_._1._1.toString)
      .mapValues(_.size)
  }

  /**
    * Return the average pixel value from a target raster and a MultiPolygon
    * area of interest.
    *
    * @param   targetLayer   The target TileLayerCollection
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A one element map averaging the pixel values
    */
  private def rasterAverage(
    targetLayer: TileLayerCollection[SpatialKey],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Double] = {
    val update = (newValue: Double, pixelValue: (DoubleAdder, LongAdder)) => {
      pixelValue match {
        case (accumulator, counter) => accumulator.add(newValue); counter.increment()
      }
    }

    val metadata = targetLayer.metadata
    val pixelValue = ( new DoubleAdder, new LongAdder )

    targetLayer.par.foreach({ case (key, tile) =>
      val re = RasterExtent(metadata.mapTransform(key), metadata.layout.tileCols,
        metadata.layout.tileRows)

      Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
        val targetLayerData = tile.getDouble(col, row)

        val targetLayerValue =
          if (isData(targetLayerData)) targetLayerData
          else 0.0

        update(targetLayerValue, pixelValue)
      }
    })

    pixelValue match {
      case (accumulator, counter) => Map("List(0)" -> accumulator.sum / counter.sum)
    }
  }

  /**
    * Return the average pixel value from a target raster and a MultiPolygon
    * area of interest.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   targetLayer   The target TileLayerCollection
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A map of targetRaster pixel value averages
    */
  private def rasterGroupedAverage(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    targetLayer: TileLayerCollection[SpatialKey],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Double] = {
    val init = () => ( new DoubleAdder, new LongAdder )
    val update = (newValue: Double, pixelValue: (DoubleAdder, LongAdder)) => {
      pixelValue match {
        case (accumulator, counter) => accumulator.add(newValue); counter.increment()
      }
    }

    val metadata = targetLayer.metadata
    val pixelGroups: TrieMap[List[Int], (DoubleAdder, LongAdder)] = TrieMap.empty

    joinCollectionLayers(targetLayer +: rasterLayers).par
      .foreach({ case (key, targetTile :: tiles) =>
        val extent: Extent = metadata.mapTransform(key)
        val re: RasterExtent = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixelKey: List[Int] = tiles.map(_.get(col, row)).toList
          val pixelValues = pixelGroups.getOrElseUpdate(pixelKey, init())
          val targetLayerData = targetTile.getDouble(col, row)

          val targetLayerValue =
            if (isData(targetLayerData)) targetLayerData
            else 0.0

          update(targetLayerValue, pixelValues)
        }
      })

    pixelGroups
      .mapValues { case (accumulator, counter) => accumulator.sum / counter.sum }
      .map { case (k, v) => k.toString -> v }
      .toMap
  }

  /**
    * From a sequence of rasterLayers and a shape, return a list of pixel counts.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A Map of cell counts
    */
  private def rasterGroupedCount(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Int] = {
    val init = () => new LongAdder
    val update = (_: LongAdder).increment()
    // assume all the layouts are the same
    val metadata = rasterLayers.head.metadata

    val pixelGroups: TrieMap[List[Int], LongAdder] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent: Extent = metadata.mapTransform(key)
        val re: RasterExtent = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixelGroup: List[Int] = tiles.map(_.get(col, row)).toList
          val acc = pixelGroups.getOrElseUpdate(pixelGroup, init())
          update(acc)
        }
      })

    pixelGroups
      .mapValues(_.sum().toInt)
      .map { case (k, v) => k.toString -> v}
      .toMap
  }

  type RasterSummary = (DoubleAccumulator, DoubleAdder, DoubleAccumulator, LongAdder)

  /**
    * From a list of rasters and a shape, return a list of maps containing min,
    * avg, and max values of those rasters.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A Seq of Map of min, avg, and max values
    */
  private def rasterSummary(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Seq[Map[String, Double]] = {
    val update = (newValue: Double, rasterSummary: RasterSummary) => {
      rasterSummary match {
        case (min, sum, max, count) =>
          min.accumulate(newValue)
          sum.add(newValue)
          max.accumulate(newValue)
          count.increment()
      }
    }

    val init = () => (
      new DoubleAccumulator(new MinWithoutNoData, Double.MaxValue),
      new DoubleAdder,
      new DoubleAccumulator(new MaxWithoutNoData, Double.MinValue),
      new LongAdder
    )

    // assume all layouts are the same
    val metadata = rasterLayers.head.metadata

    val layerSummaries: TrieMap[Int, RasterSummary] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent = metadata.mapTransform(key)
        val re = RasterExtent(extent, metadata.tileLayout.tileCols, metadata.tileLayout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixels: List[Double] = tiles.map(_.getDouble(col, row)).toList
          pixels.zipWithIndex.foreach { case (pixel, index) =>
            val rasterSummary = layerSummaries.getOrElseUpdate(index, init())
            update(pixel, rasterSummary)
          }
        }
      })

    layerSummaries
      .toSeq
      .sortBy(_._1)
      .map { case (_, (min, sum, max, count)) =>
        Map(
          "min" -> min.get(),
          "avg" -> sum.sum() / count.sum(),
          "max" -> max.get()
        )
      }
  }
}
