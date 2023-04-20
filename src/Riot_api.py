from typing import Optional, List, Dict, Any
import requests


class RiotAPI:
    def __init__(self, api_key: str, region: str) -> None:
        self.api_key = api_key
        self.region = region
        self.headers = {'X-Riot-Token': api_key}

    def get_summoner(self, summoner_name: str) -> Optional[Dict[str, str]]:
        url = f'https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}'
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f'Request failed with status code: {response.status_code}')
            return None
        
    def get_match_history(self, summoner_name: str, region: str) -> Optional[List[str]]:
        summoner_data = self.get_summoner(summoner_name)
        if summoner_data:
            puuid = summoner_data['puuid']
            url = f'https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids'
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f'Request failed with status code: {response.status_code}')
                return None
        else:
            return None
        
    def get_match_details(self, match_id: str, region: str) -> Optional[Dict[str, Any]]:
        url = f'https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}'
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f'Request failed with status code: {response.status_code}')
            return None
        
    def get_last_20_champions_played(self, summoner_name: str, region: str) -> List[int]:
        champions = []
        match_history = self.get_match_history(summoner_name, region)
        if match_history:
            for match_id in match_history[:20]:
                match_details = self.get_match_details(match_id, region)
                if match_details:
                    puuid = self.get_summoner(summoner_name)['puuid']
                    for participant in match_details['info']['participants']:
                        if participant['puuid'] == puuid:
                            champions.append(participant['championId'])
        return champions

    def get_last_20_match_results(self, summoner_name: str, region: str) -> List[Dict[str, Any]]:
        results = []
        match_history = self.get_match_history(summoner_name, region)
        if match_history:
            for match_id in match_history[:20]:
                match_details = self.get_match_details(match_id, region)
                if match_details:
                    puuid = self.get_summoner(summoner_name)['puuid']
                    for participant in match_details['info']['participants']:
                        if participant['puuid'] == puuid:
                            result = {
                                'win': participant['win'],
                                'kills': participant['kills'],
                                'deaths': participant['deaths'],
                                'assists': participant['assists']
                            }
                            results.append(result)
        return results


class DataDragonAPI:
    def __init__(self) -> None:
        # Get the latest version of the Data Dragon API
        versions_url = 'https://ddragon.leagueoflegends.com/api/versions.json'
        versions_response = requests.get(versions_url)
        self.latest_version = versions_response.json()[0]

    def get_champion_data(self) -> Dict[str, Dict[str, str]]:
        # Get the champion data for the latest version
        champions_url = f'http://ddragon.leagueoflegends.com/cdn/{self.latest_version}/data/en_US/champion.json'
        champions_response = requests.get(champions_url)
        champions_data = champions_response.json()
        return champions_data['data']

    def get_champion_name(self, champion_id: int) -> str:
        # Find the champion with the given ID
        champions_data = self.get_champion_data()
        for champion in champions_data.values():
            if int(champion['key']) == champion_id:
                return champion['name']

        # Return None if the champion was not found
        return None

    def get_item_data(self) -> Dict[str, Dict[str, str]]:
        # Get the item data for the latest version
        items_url = f'http://ddragon.leagueoflegends.com/cdn/{self.latest_version}/data/en_US/item.json'
        items_response = requests.get(items_url)
        items_data = items_response.json()
        return items_data['data']

    def get_item_name(self, item_id: int) -> str:
        # Find the item with the given ID
        items_data = self.get_item_data()
        item = items_data.get(str(item_id))
        if item:
            return item['name']

        # Return None if the item was not found
        return None