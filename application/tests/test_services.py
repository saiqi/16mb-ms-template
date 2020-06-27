import pytest

import json
from nameko.testing.services import worker_factory

from application.services.template import TemplateService

@pytest.fixture
def template():
    return """
    {
        "id": "dsa_fbl_mt_duel",
        "name": "Statistiques du match",
        "language": "FR",
        "context": "soccer",
        "bundle": "dsa_football_post_match",
        "creation_date": "2019-06-02T16:12:10.821Z",
        "picture": {
            "context": "default"
        },
        "kind": "image",
        "datasource": null,
        "queries": [
            {
                "id": "soccer_match_infos",
                "referential_parameters": [
                    {
                        "match_id": {
                            "name": "match",
                            "event_or_entity": "event",
                            "label": "Match"
                        }
                    }
                ],
                "labels": {
                    "type": "label"
                },
                "referential_results": null,
                "user_parameters": null,
                "limit": 50
            },
            {
                "id": "soccer_match_team_infos",
                "referential_parameters": [
                    {
                        "match_id": {
                            "name": "match",
                            "event_or_entity": "event"
                        }
                    }
                ],
                "labels": null,
                "referential_results": {
                    "team_id": {
                        "event_or_entity": "entity",
                        "column_id": "side",
                        "picture": {
                            "format": "standard"
                        }
                    },
                    "competition_id": {
                        "event_or_entity": "entity",
                        "column_id": "competition_flag",
                        "picture": {
                            "format": "standard",
                            "kind": "vectorial"
                        }
                    }
                },
                "user_parameters": null,
                "limit": 50
            },
            {
                "id": "soccer_match_team_stats",
                "referential_parameters": [
                    {
                        "match_id": {
                            "name": "match",
                            "event_or_entity": "event"
                        }
                    }
                ],
                "labels": {
                    "type": "label"
                },
                "referential_results": null,
                "user_parameters": null,
                "limit": 50
            }
        ],
        "allowed_users": [
            "my_user"
        ],
        "svg": "<svg></svg>"
    }
    """

@pytest.fixture
def queries():
    def get_query(query_id):
        if query_id == 'soccer_match_infos':
            return """
            {
                "id": "soccer_match_infos",
                "name": "Soccer match infos",
                "sql": "SELECT M.ATTENDANCE, M.POOL, EXTRACT(DAY FROM M.DATE) AS DATE_DAY, EXTRACT(MONTH FROM M.DATE) AS DATE_MONTH, EXTRACT(YEAR FROM M.DATE) AS DATE_YEARFROM SOCCER_MATCHINFO MWHERE M.ID = %s",
                "parameters": [
                    "match_id"
                ],
                "creation_date": "2019-06-02T16:13:20.033Z"
            }
            """
        if query_id == 'soccer_match_team_infos':
            return """
            {
                "id": "soccer_match_team_infos",
                "sql": "SELECT SIDE, TEAM_ID, SCORE, COMPETITION_ID, 'competition' AS COMPETITION_FLAG FROM SOCCER_TEAMSTAT WHERE MATCH_ID = %s AND TYPE = 'touches' ",
                "name": "Soccer match team infos",
                "creation_date": "2019-06-02T16:13:20.033Z",
                "parameters": [
                    "match_id"
                ]
            }
            """
        return """
        {
            "id": "soccer_match_team_stats",
            "name": "Soccer match team stats",
            "sql": "WITH STATS AS (  SELECT  S.TYPE  , CASE WHEN HS.VALUE IS NULL THEN 0 ELSE HS.VALUE END AS HOME_VALUE  , CASE WHEN AWS.VALUE IS NULL THEN 0 ELSE AWS.VALUE END AS AWAY_VALUE  FROM USER_SOCCER_TEAM_STAT S  JOIN (    SELECT DISTINCT MATCH_ID FROM SOCCER_TEAMSTAT WHERE MATCH_ID = %s AND TYPE = 'touches'  ) TS ON 1=1  LEFT JOIN SOCCER_TEAMSTAT HS ON (HS.TYPE = S.TYPE AND HS.SIDE = 'Home' AND HS.MATCH_ID = TS.MATCH_ID)  LEFT JOIN SOCCER_TEAMSTAT AWS ON (AWS.TYPE = S.TYPE AND AWS.SIDE = 'Away' AND TS.MATCH_ID = AWS.MATCH_ID)),POSSESSION AS (  SELECT TYPE, HOME_VALUE/100. AS HOME_VALUE, AWAY_VALUE/100. AS AWAY_VALUE, TRUE AS IS_SUCCESS_RATE, 1 AS TOTAL  FROM STATS WHERE TYPE = 'possession_percentage'),DUEL_RATE AS (  SELECT SUC.HOME_VALUE / (FAI.HOME_VALUE + SUC.HOME_VALUE) AS HOME_VALUE  , SUC.AWAY_VALUE / (FAI.AWAY_VALUE + SUC.AWAY_VALUE) AS AWAY_VALUE  , TRUE AS IS_SUCCESS_RATE  , 1 AS TOTAL  FROM STATS SUC, STATS FAI  WHERE SUC.TYPE = 'duel_won' AND FAI.TYPE = 'duel_lost'),PASS_RATE AS (  SELECT SUC.HOME_VALUE / TOT.HOME_VALUE AS HOME_VALUE  , SUC.AWAY_VALUE / TOT.AWAY_VALUE AS AWAY_VALUE  , TRUE AS IS_SUCCESS_RATE  , 1 AS TOTAL  FROM STATS SUC, STATS TOT  WHERE SUC.TYPE = 'accurate_pass' AND TOT.TYPE = 'total_pass'),AERIAL_DUEL_RATE AS (  SELECT SUC.HOME_VALUE / (FAI.HOME_VALUE + SUC.HOME_VALUE) AS HOME_VALUE  , SUC.AWAY_VALUE / (FAI.AWAY_VALUE + SUC.AWAY_VALUE) AS AWAY_VALUE  , TRUE AS IS_SUCCESS_RATE  , 1 AS TOTAL  FROM STATS SUC, STATS FAI  WHERE SUC.TYPE = 'aerial_won' AND FAI.TYPE = 'aerial_lost'),GROUND_DUEL_RATE AS (  SELECT (DW.HOME_VALUE - AW.HOME_VALUE) / (DW.HOME_VALUE - AW.HOME_VALUE + DL.HOME_VALUE - AL.HOME_VALUE) AS HOME_VALUE  , (DW.AWAY_VALUE - AW.AWAY_VALUE) / (DW.AWAY_VALUE - AW.AWAY_VALUE + DL.AWAY_VALUE - AL.AWAY_VALUE) AS AWAY_VALUE  , TRUE AS IS_SUCCESS_RATE  , 1 AS TOTAL  FROM STATS DW, STATS DL, STATS AW, STATS AL  WHERE AW.TYPE = 'aerial_won' AND AL.TYPE = 'aerial_lost'  AND DW.TYPE = 'duel_won' AND DL.TYPE = 'duel_lost')SELECTTYPE, HOME_VALUE, AWAY_VALUE, IS_SUCCESS_RATE, TOTAL, 0 AS RANKFROM POSSESSIONUNION ALLSELECTTYPE, HOME_VALUE, AWAY_VALUE, FALSE AS IS_SUCCESS_RATE, HOME_VALUE + AWAY_VALUE AS TOTAL, 10 + RANK() OVER (ORDER BY TYPE DESC) AS RANKFROM STATSWHERE TYPE IN ('total_scoring_att', 'ontarget_scoring_att', 'total_pass')UNION ALLSELECT'pass_success_rate' AS TYPE, HOME_VALUE, AWAY_VALUE, IS_SUCCESS_RATE, TOTAL, 20 AS RANKFROM PASS_RATEUNION ALLSELECT'duel_success_rate' AS TYPE, HOME_VALUE, AWAY_VALUE, IS_SUCCESS_RATE, TOTAL, 30 AS RANKFROM DUEL_RATEUNION ALLSELECTTYPE, HOME_VALUE, AWAY_VALUE, FALSE AS IS_SUCCESS_RATE, HOME_VALUE + AWAY_VALUE AS TOTAL, 40 + RANK() OVER (ORDER BY TYPE DESC) AS RANK FROM STATSWHERE TYPE IN ('total_offside', 'accurate_cross', 'fk_foul_lost', 'won_contest')UNION ALLSELECT'aerial_success_rate' AS TYPE, HOME_VALUE, AWAY_VALUE, IS_SUCCESS_RATE, TOTAL, 50 AS RANKFROM AERIAL_DUEL_RATEUNION ALLSELECT'ground_duel_success_rate' AS TYPE, HOME_VALUE, AWAY_VALUE, IS_SUCCESS_RATE, TOTAL, 60 AS RANKFROM GROUND_DUEL_RATE",
            "parameters": [
                "match_id"
            ],
            "creation_date": "2019-06-02T16:13:30.336Z"
        }
        """
    return get_query

@pytest.fixture
def event():
    return """
    {
        "id" : "f985507",
        "date" : "2019-05-03T18:45:00Z",
        "provider" : "opta_f9",
        "type" : "game",
        "common_name" : "Strasbourg - Marseille",
        "content" : {
            "venue" : "Stade de la Meinau",
            "country" : "France",
            "competition" : "French Ligue 1",
            "season" : " 2018/2019",
            "name" : "Strasbourg - Marseille"
        },
        "entities" : [
            {
                "id" : "c24",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "c24",
                    "name" : "French Ligue 1",
                    "country" : "France",
                    "code" : "FR_L1"
                },
                "common_name" : "French Ligue 1",
                "type" : "soccer competition"
            },
            {
                "id" : "2018",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "2018",
                    "name" : " 2018/2019"
                },
                "common_name" : " 2018/2019",
                "type" : "soccer season"
            },
            {
                "id" : "v1320",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "v1320",
                    "name" : "Stade de la Meinau",
                    "country" : "France"
                },
                "common_name" : "Stade de la Meinau",
                "type" : "soccer venue"
            },
            {
                "id" : "p85633",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p85633",
                    "first_name" : "Matz",
                    "last_name" : "Sels",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Matz Sels",
                "type" : "soccer player"
            },
            {
                "id" : "p232240",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p232240",
                    "first_name" : "Anthony",
                    "last_name" : "Caci",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Anthony Caci",
                "type" : "soccer player"
            },
            {
                "id" : "p112945",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p112945",
                    "first_name" : "Stefan",
                    "last_name" : "Mitrovic",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Stefan Mitrovic",
                "type" : "soccer player"
            },
            {
                "id" : "p45076",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p45076",
                    "first_name" : "Lamine",
                    "last_name" : "Koné",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Lamine Koné",
                "type" : "soccer player"
            },
            {
                "id" : "p84366",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p84366",
                    "first_name" : "Jonas",
                    "last_name" : "Martin",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Jonas Martin",
                "type" : "soccer player"
            },
            {
                "id" : "p92527",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p92527",
                    "first_name" : "Kenny",
                    "last_name" : "Lala",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Kenny Lala",
                "type" : "soccer player"
            },
            {
                "id" : "p226451",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p226451",
                    "first_name" : "Ibrahima",
                    "last_name" : "Sissoko",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Ibrahima Sissoko",
                "type" : "soccer player"
            },
            {
                "id" : "p86413",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p86413",
                    "first_name" : "Lionel",
                    "last_name" : "Carole",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Lionel Carole",
                "type" : "soccer player"
            },
            {
                "id" : "p461913",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p461913",
                    "first_name" : "Youssouf",
                    "last_name" : "Fofana",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Youssouf Fofana",
                "type" : "soccer player"
            },
            {
                "id" : "p177840",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p177840",
                    "first_name" : "Nuno Miguel",
                    "last_name" : "da Costa Jóia",
                    "known" : "Nuno da Costa",
                    "type" : "player"
                },
                "common_name" : "Nuno da Costa",
                "type" : "soccer player"
            },
            {
                "id" : "p168539",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p168539",
                    "first_name" : "Ludovic",
                    "last_name" : "Ajorque",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Ludovic Ajorque",
                "type" : "soccer player"
            },
            {
                "id" : "p228322",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p228322",
                    "first_name" : "Dimitri",
                    "last_name" : "Lienard",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Dimitri Lienard",
                "type" : "soccer player"
            },
            {
                "id" : "p230337",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p230337",
                    "first_name" : "Lebo",
                    "last_name" : "Mothiba",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Lebo Mothiba",
                "type" : "soccer player"
            },
            {
                "id" : "p226194",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p226194",
                    "first_name" : "Samuel",
                    "last_name" : "Grandsir",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Samuel Grandsir",
                "type" : "soccer player"
            },
            {
                "id" : "p67272",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p67272",
                    "first_name" : "Anthony",
                    "last_name" : "Gonçalves",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Anthony Gonçalves",
                "type" : "soccer player"
            },
            {
                "id" : "p133632",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p133632",
                    "first_name" : "Abdallah",
                    "last_name" : "Ndour",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Abdallah Ndour",
                "type" : "soccer player"
            },
            {
                "id" : "p194188",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p194188",
                    "first_name" : "Bingourou",
                    "last_name" : "Kamara",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Bingourou Kamara",
                "type" : "soccer player"
            },
            {
                "id" : "p115961",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p115961",
                    "first_name" : "Adrien",
                    "last_name" : "Thomasson",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Adrien Thomasson",
                "type" : "soccer player"
            },
            {
                "id" : "man37977",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "man37977",
                    "first_name" : "Thierry",
                    "last_name" : "Laurey",
                    "known" : null,
                    "type" : "manager"
                },
                "common_name" : "Thierry Laurey",
                "type" : "soccer manager"
            },
            {
                "id" : "p44413",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p44413",
                    "first_name" : "Steve",
                    "last_name" : "Mandanda",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Steve Mandanda",
                "type" : "soccer player"
            },
            {
                "id" : "p173271",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p173271",
                    "first_name" : "Duje",
                    "last_name" : "Caleta-Car",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Duje Caleta-Car",
                "type" : "soccer player"
            },
            {
                "id" : "p41795",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p41795",
                    "first_name" : "Adil",
                    "last_name" : "Rami",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Adil Rami",
                "type" : "soccer player"
            },
            {
                "id" : "p226944",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p226944",
                    "first_name" : "Boubacar",
                    "last_name" : "Kamara",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Boubacar Kamara",
                "type" : "soccer player"
            },
            {
                "id" : "p121117",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p121117",
                    "first_name" : "Lucas",
                    "last_name" : "Ocampos",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Lucas Ocampos",
                "type" : "soccer player"
            },
            {
                "id" : "p47654",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p47654",
                    "first_name" : "Luiz Gustavo",
                    "last_name" : "Dias",
                    "known" : "Luiz Gustavo",
                    "type" : "player"
                },
                "common_name" : "Luiz Gustavo",
                "type" : "soccer player"
            },
            {
                "id" : "p49887",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p49887",
                    "first_name" : "Kevin",
                    "last_name" : "Strootman",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Kevin Strootman",
                "type" : "soccer player"
            },
            {
                "id" : "p109404",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p109404",
                    "first_name" : "Hiroki",
                    "last_name" : "Sakai",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Hiroki Sakai",
                "type" : "soccer player"
            },
            {
                "id" : "p213965",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p213965",
                    "first_name" : "Maxime",
                    "last_name" : "Lopez",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Maxime Lopez",
                "type" : "soccer player"
            },
            {
                "id" : "p42493",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p42493",
                    "first_name" : "Mario",
                    "last_name" : "Balotelli",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Mario Balotelli",
                "type" : "soccer player"
            },
            {
                "id" : "p97041",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p97041",
                    "first_name" : "Valère",
                    "last_name" : "Germain",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Valère Germain",
                "type" : "soccer player"
            },
            {
                "id" : "p37901",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p37901",
                    "first_name" : "Dimitri",
                    "last_name" : "Payet",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Dimitri Payet",
                "type" : "soccer player"
            },
            {
                "id" : "p115380",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p115380",
                    "first_name" : "Jordan",
                    "last_name" : "Amavi",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Jordan Amavi",
                "type" : "soccer player"
            },
            {
                "id" : "p195908",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p195908",
                    "first_name" : "Nemanja",
                    "last_name" : "Radonjic",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Nemanja Radonjic",
                "type" : "soccer player"
            },
            {
                "id" : "p37769",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p37769",
                    "first_name" : "Yohann",
                    "last_name" : "Pelé",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Yohann Pelé",
                "type" : "soccer player"
            },
            {
                "id" : "p145212",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p145212",
                    "first_name" : "Clinton",
                    "last_name" : "N'Jie",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Clinton N'Jie",
                "type" : "soccer player"
            },
            {
                "id" : "p122775",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p122775",
                    "first_name" : "Morgan",
                    "last_name" : "Sanson",
                    "known" : null,
                    "type" : "player"
                },
                "common_name" : "Morgan Sanson",
                "type" : "soccer player"
            },
            {
                "id" : "p39294",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "p39294",
                    "first_name" : "Rolando Jorge",
                    "last_name" : "Pires da Fonseca",
                    "known" : "Rolando",
                    "type" : "player"
                },
                "common_name" : "Rolando",
                "type" : "soccer player"
            },
            {
                "id" : "man37563",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "man37563",
                    "first_name" : "Rudi",
                    "last_name" : "Garcia",
                    "known" : null,
                    "type" : "manager"
                },
                "common_name" : "Rudi Garcia",
                "type" : "soccer manager"
            },
            {
                "id" : "o41567",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "o41567",
                    "first_name" : "Jerome",
                    "last_name" : "Miguelgorry",
                    "known" : null,
                    "type" : "referee"
                },
                "common_name" : "Jerome Miguelgorry",
                "type" : "soccer referee"
            },
            {
                "id" : "t153",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "t153",
                    "name" : "Strasbourg",
                    "country" : "France"
                },
                "common_name" : "Strasbourg",
                "type" : "soccer team"
            },
            {
                "id" : "t144",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "t144",
                    "name" : "Marseille",
                    "country" : "France"
                },
                "common_name" : "Marseille",
                "type" : "soccer team"
            }
        ],
        "allowed_users" : [
            "my_user"
        ]
    }
    """

@pytest.fixture
def entities():
    def get_entity_by_id(entity_id, user):
        if entity_id == "t144":
            return """
            {
                "id" : "t144",
                "provider" : "opta_f9",
                "informations" : {
                    "id" : "t144",
                    "name" : "Marseille",
                    "country" : "France"
                },
                "common_name" : "Marseille",
                "type" : "soccer team"
            }
            """
        return """
        {
            "id" : "t153",
            "provider" : "opta_f9",
            "informations" : {
                "id" : "t153",
                "name" : "Strasbourg",
                "country" : "France"
            },
            "common_name" : "Strasbourg",
            "type" : "soccer team"
        }
        """
    return get_entity_by_id

@pytest.fixture
def query_results():
    def select(query, parameters, limit):
        if query.startswith("WITH"):
            return """
            [
                {
                    "type": "possession_percentage",
                    "home_value": 0.435,
                    "away_value": 0.565,
                    "is_success_rate": true,
                    "total": 1,
                    "rank": 0
                },
                {
                    "type": "total_scoring_att",
                    "home_value": 7,
                    "away_value": 13,
                    "is_success_rate": false,
                    "total": 20,
                    "rank": 11
                },
                {
                    "type": "total_pass",
                    "home_value": 412,
                    "away_value": 533,
                    "is_success_rate": false,
                    "total": 945,
                    "rank": 12
                },
                {
                    "type": "ontarget_scoring_att",
                    "home_value": 4,
                    "away_value": 2,
                    "is_success_rate": false,
                    "total": 6,
                    "rank": 13
                },
                {
                    "type": "pass_success_rate",
                    "home_value": 0.8252427184466019,
                    "away_value": 0.8536585365853658,
                    "is_success_rate": true,
                    "total": 1,
                    "rank": 20
                },
                {
                    "type": "duel_success_rate",
                    "home_value": 0.46236559139784944,
                    "away_value": 0.5376344086021505,
                    "is_success_rate": true,
                    "total": 1,
                    "rank": 30
                },
                {
                    "type": "won_contest",
                    "home_value": 10,
                    "away_value": 8,
                    "is_success_rate": false,
                    "total": 18,
                    "rank": 41
                },
                {
                    "type": "total_offside",
                    "home_value": 3,
                    "away_value": 2,
                    "is_success_rate": false,
                    "total": 5,
                    "rank": 42
                },
                {
                    "type": "fk_foul_lost",
                    "home_value": 12,
                    "away_value": 15,
                    "is_success_rate": false,
                    "total": 27,
                    "rank": 43
                },
                {
                    "type": "accurate_cross",
                    "home_value": 3,
                    "away_value": 4,
                    "is_success_rate": false,
                    "total": 7,
                    "rank": 44
                },
                {
                    "type": "aerial_success_rate",
                    "home_value": 0.48,
                    "away_value": 0.52,
                    "is_success_rate": true,
                    "total": 1,
                    "rank": 50
                },
                {
                    "type": "ground_duel_success_rate",
                    "home_value": 0.45588235294117646,
                    "away_value": 0.5441176470588235,
                    "is_success_rate": true,
                    "total": 1,
                    "rank": 60
                }
            ]
            """
        if query.startswith("SELECT SIDE"):
            return """
            [
                {
                    "side": "Home",
                    "team_id": "t153",
                    "score": 1,
                    "competition_id": "c24",
                    "competition_flag": "competition"
                },
                {
                    "side": "Away",
                    "team_id": "t144",
                    "score": 1,
                    "competition_id": "c24",
                    "competition_flag": "competition"
                }
            ]
            """
        return """
        [{"attendance": 25962, "pool": null, "date_day": 3, "date_month": 5, "date_year": 2019}]
        """
    return select

@pytest.fixture
def triggers():
    return """
    [
        {
            "id": "dsa_troyes_mt_duel",
            "name": "DSAS Troyes team stats",
            "on_event": {
                "source": "opta",
                "type": "f9"
            },
            "selector": [
                "t144"
            ],
            "template": {
                "id": "dsa_fbl_mt_duel",
                "json_only": false,
                "language": "FR",
                "referential": {
                    "match": {
                        "from_event": true
                    }
                }
            },
            "user": "my_user",
            "export": {
                "format": "png",
                "filename": "export.png"
            }
        },
        {
            "id": "dsa_troyes_mt_duel_2",
            "name": "DSAS Troyes team stats",
            "on_event": {
                "source": "opta",
                "type": "f9"
            },
            "selector": [
                "t144"
            ],
            "template": {
                "id": "dsa_fbl_mt_duel",
                "json_only": false,
                "language": "FR",
                "referential": {
                    "match": {
                        "from_event": true
                    }
                }
            },
            "user": "my_user",
            "export": {
                "format": "png"
            }
        }    
    ]
    """

@pytest.fixture
def subscription():
    return """
    {
        "user": "my_user",
        "subscription": {
            "export": {
                "target": {
                    "type": "s3",
                    "config": {"bucket": "my_bucket"}
                }
            },
            "pictures": ["default"],
            "notification": {
                "type": "slack",
                "config": {"channel": "my_channel"}
            }
        }
    }
    """

def test_resolve(template, queries, event, entities, query_results):
    service = worker_factory(TemplateService)
    service.metadata.get_template.return_value = template
    service.metadata.get_query.side_effect = queries
    service.referential.get_event_by_id.return_value = event
    service.referential.get_entity_by_id.side_effect = entities
    service.datareader.select.side_effect = query_results
    service.referential.get_labels_by_id_and_language_and_context.return_value = {'label': 'mylabel'}
    service.referential.get_entity_picture.return_value = 'picture'
    service.resolve('dsa_fbl_mt_duel', 'default', 'FR',
                    False, {'match': {'id': 'f985507', 'event_or_entity': 'event'}}, None, 'my_user', True)

def test_handle_input_loaded(triggers, template, queries, event, entities, query_results, subscription):
    service = worker_factory(TemplateService)
    service.metadata.get_template.return_value = template
    service.metadata.get_query.side_effect = queries
    service.referential.get_event_by_id.return_value = event
    service.referential.get_entity_by_id.side_effect = entities
    service.datareader.select.side_effect = query_results
    service.referential.get_labels_by_id_and_language_and_context.return_value = {'label': 'mylabel'}
    service.referential.get_entity_picture.return_value = 'picture'
    service.metadata.get_fired_triggers.return_value = triggers
    service.referential.get_event_filtered_by_entities.return_value = event
    service.subscription.get_subscription_by_user.return_value = subscription
    service.handle_input_loaded('{"meta": {"source": "opta", "type": "f9", "content_id": "f985507"}, "id": "985507"}')