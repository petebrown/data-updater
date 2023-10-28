import pandas as pd
import requests
import re
import os


def get_headers():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/90.0.4430.212 Safari/537.36"
    }
    return headers


class fixtures:
    def __init__(self, input_date=None):
        if input_date:
            self.date = (
                pd.to_datetime(input_date).tz_localize("Europe/London").normalize()
            )
        else:
            self.date = pd.Timestamp.today(tz="Europe/London").normalize()
        self.uk_datetime = pd.Timestamp.now(tz="Europe/London")

        self.url = (
            "https://www.soccerbase.com/teams/team.sd?team_id=2598&teamTabs=results"
        )

        self.r = requests.get(self.url, headers=get_headers())
        self.tables = pd.read_html(self.r.content, flavor="bs4")
        self.df = self.tables[1].reset_index(drop=True)[:-6]

        self.all = self.clean_df()
        self.list = self.clean_df()
        self.today = self.get_today()
        self.ready = self.get_ready()
        self.date_ready = self.get_date_to_scrape()
        self.played = self.get_played()
        self.unplayed = self.get_unplayed()
        self.fixture = self.get_today()

    def clean_df(self):
        df = self.df.copy()
        df.columns = df.columns.droplevel(0)
        col = df.Competition

        df["date_time"] = pd.to_datetime(df.Competition.str[-16:]).dt.tz_localize(
            "Europe/London"
        )
        df["end_time"] = df.date_time + pd.Timedelta(hours=2.25)

        df["game_date"] = pd.to_datetime(
            col.str.split(" ", expand=False).str[-2]
        ).dt.tz_localize("Europe/London")
        df["day"] = df.game_date.dt.day_name()
        df["ko_time"] = col.str.split(" ", expand=False).str[-1]

        df["competition"] = (
            col.str.split(" ", expand=False)
            .str[:-2]
            .apply(lambda x: x[: len(x) // 2])
            .str.join(" ")
        )

        df["venue"] = df.Home.apply(lambda x: "H" if "Tranmere" in x else "A")

        df["opposition"] = df.apply(
            lambda x: x.Away[:-19] if x.venue == "H" else x.Home[:-19], axis=1
        )

        df = df[
            [
                "day",
                "game_date",
                "ko_time",
                "opposition",
                "venue",
                "competition",
                "date_time",
                "end_time",
            ]
        ]

        return df

    def get_today(self):
        df = self.all.copy()
        df = df[df.game_date == self.date].reset_index(drop=True)
        return df

    def get_ready(self):
        df = self.today.copy()
        df = df[df.end_time < self.uk_datetime].reset_index(drop=True)
        return df

    def get_date_to_scrape(self):
        df = self.ready.copy()
        if not df.empty:
            game_date = df.game_date.dt.date.astype(str).values[0]
            return game_date

    def get_played(self):
        df = self.all.copy()
        df = df[df.end_time < self.uk_datetime].reset_index(drop=True)
        return df

    def get_unplayed(self):
        df = self.all.copy()
        df = df[df.end_time > self.uk_datetime].reset_index(drop=True)
        return df


class league_table:
    def __init__(self, date, pre_match=False, venue=None):
        if pre_match is True:
            self.date = self.get_prematch_date(date)
        else:
            self.date = date
        self.venue = venue

        self.url = self.get_url()

        r = requests.get(self.url, headers=get_headers())

        self.tables = pd.read_html(r.content, flavor="bs4")

        self.table = self.get_table()
        self.pos = self.get_pos()
        self.pts = self.get_pts()

    
    def get_prematch_date(self, date):
        prev_day = pd.to_datetime(date) - pd.Timedelta(days=1)
        prev_day = prev_day.strftime("%Y-%m-%d")
        return prev_day
    
    
    def get_url(self):
        year = self.date[:4]
        month = pd.to_datetime(self.date).month_name().lower()
        day = self.date[8:]

        url = f"https://www.11v11.com/league-tables/league-two/{day}-{month}-{year}/"
        if self.venue and self.venue[0].upper() in ["H", "A"]:
            if self.venue[0].upper() == "H":
                self.venue = "home"
            else:
                self.venue = "away"
            url += f"{self.venue}"
        return url

    def get_table(self):
        table = self.tables[0]
        table.Pos = table.Pos.index + 1
        return table

    def get_pos(self):
        try:
            pos = self.table.query("Team.str.contains('Tranmere')").Pos.values[0]
        except:
            pos = "No table containing Tranmere Rovers found"
        return pos

    def get_pts(self):
        try:
            pts = self.table.query("Team.str.contains('Tranmere')").Pts.values[0]
        except:
            pts = "No table containing Tranmere Rovers found"
        return pts


class bbc_api:
    def __init__(self, date):
        self.date = date

        self.match_url = self.get_match_url()
        self.match_list = self.get_match_list()
        try:
            self.match_key = self.get_match_key()
            self.event_key = self.get_event_key()
            self.tournament_data = self.get_tournament_data()
            self.match_data = self.get_match_data()
            self.cup_data = self.get_cup_data()
            self.tranmere, self.opponent = self.get_teams()
            self.opp_name = self.match_data[self.opponent]["name"]["full"]
            self.teams = self.print_teams()
            self.goals_for, self.goals_against, self.score = self.get_score()

            self.lineup_url = self.get_lineup_url()
            self.lineup_data = self.get_lineup_data()
            self.tranmere_players = self.lineup_data["teams"][self.tranmere]["players"]
            self.opp_players = self.lineup_data["teams"][self.opponent]["players"]
            self.attendance = self.get_attendance()
            self.referee = self.get_referee()
            self.stadium = self.get_stadium()
            self.venue = self.get_venue()
            self.ko_time = self.get_ko_time()
            self.formation = self.get_formation()
        except:
            return None

    def get_match_url(self):
        url = f"https://push.api.bbci.co.uk/data/bbc-morph-football-scores-match-list-data/endDate/{self.date}/startDate/{self.date}/team/tranmere-rovers/todayDate/{self.date}/version/2.4.6/withPlayerActions/true?timeout=5"
        return url

    def get_match_list(self):
        r = requests.get(self.match_url, headers=get_headers())
        match_list = r.json()

        if not match_list["matchData"]:
            print(f"No matches found for {self.date}")
            return
        else:
            return match_list

    def get_match_key(self):
        match_key = next(
            iter(self.match_list["matchData"][0]["tournamentDatesWithEvents"])
        )
        return match_key

    def get_event_key(self):
        event_key = self.match_list["matchData"][0]["tournamentDatesWithEvents"][
            self.match_key
        ][0]["events"][0]["eventKey"]
        return event_key

    def get_tournament_data(self):
        tournament_data = self.match_list["matchData"][0]["tournamentMeta"]
        return tournament_data

    def get_cup_data(self):
        cup_data = self.match_list["matchData"][0]["tournamentDatesWithEvents"][
            self.match_key
        ][0]["round"]
        return cup_data

    def get_match_data(self):
        match_data = self.match_list["matchData"][0]["tournamentDatesWithEvents"][
            self.match_key
        ][0]["events"][0]
        return match_data

    def get_lineup_url(self):
        url = f"https://push.api.bbci.co.uk/data/bbc-morph-sport-football-team-lineups-data/event/{self.event_key}/version/1.0.8"
        return url

    def get_lineup_data(self):
        r = requests.get(self.lineup_url, headers=get_headers())
        lineup_data = r.json()
        return lineup_data

    def get_teams(self):
        if self.match_data["homeTeam"]["name"]["full"] == "Tranmere Rovers":
            tranmere = "homeTeam"
            opponent = "awayTeam"
        else:
            tranmere = "awayTeam"
            opponent = "homeTeam"
        return tranmere, opponent

    def print_teams(self):
        second_team = self.match_data[self.opponent]["name"]["full"]
        print(f"Tranmere are {self.tranmere}. {second_team} are {self.opponent}")

    def get_score(self):
        goals_for = self.match_data[self.tranmere]["scores"]["score"]
        goals_against = self.match_data[self.opponent]["scores"]["score"]
        score = f"{goals_for}-{goals_against}"
        return goals_for, goals_against, score

    def get_attendance(self):
        if "attendance" in self.lineup_data["meta"].keys():
            attendance = self.lineup_data["meta"]["attendance"].replace(",", "")
            return attendance
        else:
            print(f"No attendance data for {self.date}")
            return None

    def get_referee(self):
        if "referee" in self.lineup_data["meta"].keys():
            attendance = self.lineup_data["meta"]["referee"].replace(",", "")
            return attendance
        else:
            print(f"No referee for {self.date}")
            return None

    def get_formation(self):
        formation = self.lineup_data["teams"][self.tranmere]["formation"]
        formation = "-".join(str(formation))
        return formation

    def get_stadium(self):
        stadium = self.match_data["venue"]["name"]["full"]
        return stadium

    def get_venue(self):
        if self.match_data["venue"]["name"]["full"] == "Wembley Stadium":
            venue = "N"
        elif self.match_data["venue"]["name"]["full"] == "Prenton Park":
            venue = "H"
        elif self.match_data["venue"]["name"]["full"] != "Prenton Park":
            venue = "A"
        return venue

    def get_ko_time(self):
        ko_time = self.match_data["startTimeInUKHHMM"]
        return ko_time


def get_season(date):
    month = int(date.split("-")[1])
    year = int(date.split("-")[0])

    if month >= 8:
        season_1 = str(year)
        season_2 = str(year + 1)[-2:]
        season = f"{season_1}/{season_2}"
    else:
        season_1 = str(year - 1)
        season_2 = str(year)[-2:]
        season = f"{season_1}/{season_2}"
    return season


def get_outcome(gf, ga):
    if gf > ga:
        return "W"
    elif gf < ga:
        return "L"
    else:
        return "D"


def get_game_type(data):
    if data.tournament_data["tournamentName"]["first"] in [
        "League One",
        "League Two",
        "National League",
    ]:
        if "round" not in data.cup_data:
            return "League"
        else:
            if "Play-offs" in data.cup_data["round"]["full"]:
                return "League Play-Off"
    else:
        return "Cup"


def get_table(date):
    lge_table = league_table(date)
    pos = lge_table.pos
    pts = lge_table.pts
    return pos, pts


def get_generic_comp(competition):
    generic_comps = {
        "Carabao Cup": "League Cup",
        "FA Cup Qualifying": "FA Cup Qualifying",
        "Isuzu FA Trophy": "FA Trophy",
        "League One": "Football League",
        "League Two": "Football League",
        "National League": "Non-League",
        "Papa John's Trophy": "Associate Members' Cup",
        "The Emirates FA Cup": "FA Cup",
    }
    return generic_comps[competition]


def get_league_tier(competition):
    league_tiers = {"League One": 3, "League Two": 4, "National League": 5}
    return league_tiers[competition]


def get_manager(date):
    df = pd.read_csv(
        "https://raw.githubusercontent.com/petebrown/pre-2023-data-prep/main/data/managers.csv",
        parse_dates=["date_from", "date_to"],
    )
    df = df[(df.date_from <= date) & (df.date_to >= date)]
    return df.manager_name.values[0]


def get_cup_leg(match_data):
    if "leg" in match_data["eventType"]:
        return match_data["eventType"][:1]
    else:
        return None


def get_cup_replay(match_data):
    event_type = match_data["eventType"]
    if event_type:
        if event_type.upper() == "REPLAY":
            return 1


def get_cup_name(cup_data):
    cup_name = cup_data["name"]
    if cup_name:
        cup_stage = cup_name["full"]
        if re.search(r"North(?:ern)?", cup_stage):
            cup_section = re.search(r"North(?:ern)?", cup_stage).group(0)
    else:
        cup_stage = None
        cup_section = None
    return cup_stage, cup_section


def get_cup_round(cup_stage):
    if cup_stage is None:
        return None
    elif " FINAL" in cup_stage.upper():
        return "F"
    elif cup_stage.upper() in ["PLAY-OFFS", "SEMI-FINALS"]:
        return "SF"
    elif "QUARTER-FINALS" in cup_stage.upper():
        return "QF"
    elif "FIFTH ROUND" in cup_stage.upper():
        return "5"
    elif "FOURTH ROUND" in cup_stage.upper():
        return "4"
    elif "THIRD ROUND" in cup_stage.upper():
        return "3"
    elif "SECOND ROUND" in cup_stage.upper():
        return "2"
    elif "FIRST ROUND" in cup_stage.upper():
        return "1"
    elif "GROUP" in cup_stage.upper():
        return "G"
    else:
        return None


def get_aet(match_data):
    if match_data["eventProgress"] == "EXTRATIMECOMPLETE":
        return 1
    else:
        return None


def get_shootout_outcome(pen_gf, pen_ga):
    if pen_gf:
        if pen_gf > pen_ga:
            pen_outcome = "W"
        elif pen_gf < pen_ga:
            return "L"
        pen_score = f"{pen_gf}-{pen_ga}"
    else:
        pen_outcome = None
        pen_score = None
    return pen_outcome, pen_score


def get_agg_outcome(agg_gf, agg_ga):
    if agg_gf:
        if agg_gf > agg_ga:
            agg_outcome = "W"
        elif agg_gf < agg_ga:
            agg_outcome = "L"
        agg_score = f"{agg_gf}-{agg_ga}"
    else:
        agg_outcome = None
        agg_score = None
    return agg_outcome, agg_score


def get_decider(match_data):
    decider = match_data["eventOutcomeType"]
    if decider == "shootout":
        return "pens"
    elif decider == "extra-time":
        return "extra time"
    else:
        return None


def get_cup_outcome(data, aet, pen_outcome, agg_outcome):
    if aet or pen_outcome or agg_outcome:
        return data.match_data[data.tranmere]["eventOutcome"].upper()[:1]
    else:
        return None


def get_outcome_desc(pen_outcome, pen_score, agg_outcome, agg_score):
    if pen_outcome:
        str_outcome = "Won" if pen_outcome == "W" else "Lost"
        if agg_outcome:
            outcome_desc = f"{agg_score}. {str_outcome} {pen_score} on pens"
        else:
            outcome_desc = f"{str_outcome} {pen_score} on pens"
    elif agg_outcome and not pen_outcome:
        str_outcome = "Won" if agg_outcome == "W" else "Lost"
        outcome_desc = f"{str_outcome} {agg_score} on agg"
    else:
        outcome_desc = None
    return outcome_desc


def get_match_df(date, data=None):
    if data:
        data = data
    else:
        data = bbc_api(date)
    try:
        match_data = data.match_data
    except:
        print("No match data available. Try a different date.")

    season = get_season(date)
    game_date = data.date
    opposition = data.opp_name
    venue = data.venue
    goals_for = data.goals_for
    goals_against = data.goals_against
    outcome = get_outcome(goals_for, goals_against)
    score = data.score
    goal_diff = goals_for - goals_against
    game_type = get_game_type(data)
    competition = (
        data.tournament_data["tournamentName"]["full"]
        .replace("Sky Bet ", "")
        .replace("Vanarama", "")
    )
    generic_comp = get_generic_comp(competition)
    league_tier = (
        get_league_tier(competition)
        if generic_comp in ["Football League", "Non-League"]
        else None
    )
    ko_time = data.ko_time

    cup_leg = get_cup_leg(match_data)
    cup_stage = get_cup_name(data.cup_data)[0]
    cup_replay = get_cup_replay(match_data)
    cup_section = get_cup_name(data.cup_data)[1]
    cup_round = get_cup_round(cup_stage)
    aet = get_aet(match_data)

    pen_gf = match_data[data.tranmere]["scores"]["shootout"]
    pen_ga = match_data[data.opponent]["scores"]["shootout"]
    pen_outcome = get_shootout_outcome(pen_gf, pen_ga)[0]
    pen_score = get_shootout_outcome(pen_gf, pen_ga)[1]

    agg_gf = match_data[data.tranmere]["scores"]["aggregate"]
    agg_ga = match_data[data.opponent]["scores"]["aggregate"]
    agg_outcome = get_agg_outcome(agg_gf, agg_ga)[0]
    agg_score = get_agg_outcome(agg_gf, agg_ga)[1]

    cup_outcome = get_cup_outcome(data, aet, pen_outcome, agg_outcome)
    decider = get_decider(match_data)
    outcome_desc = get_outcome_desc(pen_outcome, pen_score, agg_outcome, agg_score)

    manager = get_manager(date)
    attendance = data.attendance
    game_length = 90 if aet is None else 120
    stadium = data.stadium
    referee = data.referee

    league_pos = get_table(date)[0] if game_type == "League" else None
    pts = get_table(date)[1] if game_type == "League" else None

    match_record = [
        {
            "season": season,
            "game_date": game_date,
            "opposition": opposition,
            "venue": venue,
            "score": score,
            "outcome": outcome,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_diff": goal_diff,
            "game_type": game_type,
            "competition": competition,
            "generic_comp": generic_comp,
            "league_tier": league_tier,
            "league_pos": league_pos,
            "pts": pts,
            "attendance": attendance,
            "manager": manager,
            "ko_time": ko_time,
            "cup_round": cup_round,
            "cup_leg": cup_leg,
            "cup_stage": cup_stage,
            "cup_replay": cup_replay,
            "cup_section": cup_section,
            "aet": aet,
            "pen_gf": pen_gf,
            "pen_ga": pen_ga,
            "pen_outcome": pen_outcome,
            "pen_score": pen_score,
            "agg_gf": agg_gf,
            "agg_ga": agg_ga,
            "agg_outcome": agg_outcome,
            "agg_score": agg_score,
            "decider": decider,
            "cup_outcome": cup_outcome,
            "outcome_desc": outcome_desc,
            "game_length": game_length,
            "stadium": stadium,
            "referee": referee,
        }
    ]
    match_record = pd.DataFrame(match_record)
    match_record["game_date"] = pd.to_datetime(match_record.game_date)
    return match_record


class events_df:
    def __init__(self, date, data=None):
        self.date = pd.to_datetime(date)
        if data:
            self.data = data
        else:
            self.data = bbc_api(date)
        self.match_data = self.data.match_data
        self.players = self.data.tranmere_players

        self.goals = self.get_goals_df()
        self.subs = self.get_subs()[0]
        self.player_apps = self.get_apps()
        self.sub_mins = self.get_subs()[1]
        self.cards = self.get_cards()
        self.yellow_cards = self.get_yellow_cards()
        self.red_cards = self.get_red_cards()

    def get_goals_df(self):
        player_actions = self.match_data[self.data.tranmere]["playerActions"]

        goals = []
        for player in player_actions:
            player_name = player["name"]["full"]
            actions = player["actions"]
            for action in actions:
                if action["type"] == "goal":
                    goal = {
                        "game_date": self.date,
                        "player_name": player_name,
                        "goal_min": action["timeElapsed"],
                        "penalty": action["penalty"],
                        "own_goal": action["ownGoal"],
                    }
                    goals.append(goal)

        goals_df = pd.DataFrame(goals)
        return goals_df

    def get_apps(self):
        player_apps = []
        for player in self.players:
            player_name = player["name"]["full"]
            try:
                shirt_no = player["meta"]["uniformNumber"]
            except:
                shirt_no = None
            role = player["meta"]["status"].replace("bench", "sub")

            player_app = {
                "game_date": self.date,
                "player_name": player_name,
                "shirt_no": shirt_no,
                "role": role,
            }
            player_apps.append(player_app)

        apps_df = pd.DataFrame(player_apps)
        apps_df = apps_df.query(
            "player_name.isin(@self.subs.player_name) | role == 'starter'"
        )
        return apps_df

    def get_cards(self):
        player_cards = []

        for player in self.players:
            player_name = player["name"]["full"]
            cards = player["bookings"]
            if cards:
                for card in cards:
                    player_card = {
                        "game_date": self.date,
                        "player_name": player_name,
                        "minute": card["timeElapsed"],
                        "card_type": card["type"],
                    }
                    player_cards.append(player_card)

        cards_df = pd.DataFrame(player_cards)
        return cards_df

    def get_yellow_cards(self):
        cards = self.cards
        if not cards.empty:
            yellows = (
                cards.query("card_type == 'yellow-card'")
                .rename(columns={"minute": "min_yc"})
                .drop(columns=["card_type"])
            )
            if yellows.empty:
                print("No yellow cards")
                return None
            else:
                return yellows
        else:
            print("No yellow cards (No cards at all)")
            return None

    def get_red_cards(self):
        cards = self.cards
        if not cards.empty:
            reds = (
                cards.query("card_type.str.contains('red')")
                .rename(columns={"minute": "min_so"})
                .drop(columns=["card_type"])
            )
            if reds.empty:
                print("No reds cards")
            return reds
        else:
            print("No red cards (No cards at all)")
            return None

    def get_subs(self):
        player_subs = []
        player_sub_mins = []

        for player in self.players:
            player_name = player["name"]["full"]
            try:
                shirt_no = player["meta"]["uniformNumber"]
            except:
                shirt_no = None

            subs = player["substitutions"]
            if subs:
                sub_min = subs[0]["timeElapsed"]
                try:
                    sub_on_no = subs[0]["replacedBy"]["meta"]["uniformNumber"]
                except:
                    sub_on_no = None
                player_on = subs[0]["replacedBy"]["name"]["full"]

                sub_on_mins = {
                    "game_date": self.date,
                    "player_name": player_on,
                    "min_off": None,
                    "min_on": sub_min,
                }
                sub_off_mins = {
                    "game_date": self.date,
                    "player_name": player_name,
                    "min_off": sub_min,
                    "min_on": None,
                }
                player_sub_mins.extend([sub_on_mins, sub_off_mins])

                sub_on = {
                    "game_date": self.date,
                    "shirt_no": sub_on_no,
                    "player_name": player_on,
                    "on_for": shirt_no,
                    "off_for": None,
                }
                sub_off = {
                    "game_date": self.date,
                    "shirt_no": shirt_no,
                    "player_name": player_name,
                    "on_for": None,
                    "off_for": sub_on_no,
                }
                player_subs.extend([sub_on, sub_off])

        subs_df = pd.DataFrame(player_subs)
        sub_mins_df = pd.DataFrame(player_sub_mins)
        return subs_df, sub_mins_df


def get_timestamp():
    timestamp = pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d-%H%M%S")
    return timestamp


def archive_csv(df_name, old_df):
    timestamp = get_timestamp()

    archive_dir = f"./archive/{timestamp}"
    print(archive_dir)

    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
        print(f"Created archive directory at {archive_dir}")

    archive_path = f"{archive_dir}/{df_name}.csv"

    old_df.to_csv(archive_path, index=False)
    print(f"{df_name.upper()} archived to {archive_path}")


def get_existing_dates():
    dates = pd.read_csv("./data/results.csv", usecols = ["game_date"]).sort_values(by = "game_date", ascending = False)["game_date"].drop_duplicates()
    return dates


def check_dates(dates):
    f = fixtures()
    
    if dates in ["played", "all", "available"]:
        dates = f.played["game_date"].dt.date.astype(str).unique().tolist()
    else:
        if f.today.empty:
            print("No game today.")
            dates = None
        elif not f.date_ready:
            print(f"There is a game today against {f.today.opposition.values[0]}, but it is not ready for update. Please try again later.")
            dates = None
        else:
            print(f"Update available for today's game against {f.today.opposition.values[0]}")
            dates = [f.date_ready]
    return dates


def update_csv(df_name, old_df, updates):
    sort_cols = {
        "goals": ["game_date", "goal_min"],
        "player_apps": ["game_date", "role", "shirt_no"],
        "subs": ["game_date"],
        "sub_mins": ["game_date"],
        "yellow_cards": ["game_date", "min_yc"],
        "red_cards": ["game_date", "min_so"],
        "results": "game_date",
    }

    updates["game_date"] = pd.to_datetime(updates.game_date)

    updated_df = (
        pd.concat([old_df, updates])
        .sort_values(by="game_date", ascending=False)
        .reset_index(drop=True)
    )
    if df_name == "results":
        updated_df["game_no"] = (
            updated_df.sort_values(by="game_date").groupby(["season"]).cumcount() + 1
        )
        updated_df["ssn_comp_game_no"] = (
            updated_df.sort_values(by=["game_date"])
            .groupby(["season", "competition"])
            .cumcount()
            + 1
        )
        updated_df["weekday"] = updated_df.game_date.dt.day_name()

        updated_df.loc[updated_df.game_date == "2023-08-19", "attendance"] = 5594
    
    updated_df = updated_df.sort_values(sort_cols[df_name]).reset_index(drop=True)

    updated_df.to_csv(f"./data/{df_name}.csv", index=False)
    return updated_df


def update_df(df_name, updates):
    print(f"\nUpdating {df_name.upper()} dataframe...")

    old_df = pd.read_csv(f"./data/{df_name}.csv", parse_dates=["game_date"])

    if updates is None or updates.empty:
        print(f"No updates required for {df_name.upper()}.")
    else:
        print(f"{len(updates)} possible updates found...")
        
        updates = updates[~updates.game_date.isin(old_df.game_date)]

        n_updates = len(updates)

        print(f"{n_updates} updates being made to {df_name.upper()}.")

        if n_updates > 0:
            archive_csv(df_name, old_df)
            updated_df = update_csv(df_name, old_df, updates)
            return updated_df


def print_msg(date):
    msg = f"Trying to get match records for {date}."
    border = (len(msg) + 4) * "*"
    print(f"\n{border}\n* {msg} *\n{border}\n")


def main(date_req=None):
    dates = check_dates(date_req)
    existing_dates = get_existing_dates()

    if dates:
        for date in dates:
            if date in existing_dates:
                print(f"Already have record for {date}.")
            else:
                print_msg(date)

                match_data = bbc_api(date)
                res_updates = get_match_df(date, match_data)

                update_df("results", res_updates)

                events = events_df(date, match_data)

                dfs = [
                    "player_apps",
                    "subs",
                    "sub_mins",
                    "goals",
                    "yellow_cards",
                    "red_cards",
                ]
                for df in dfs:
                    updates = getattr(events, df)
                    update_df(df, updates)


date_type = "today"

main(date_type)
