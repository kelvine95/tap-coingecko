"""Stream for extracting a daily snapshot of comprehensive coin profile data."""

from typing import Any, Dict, Iterable, Optional, Mapping

import pendulum
import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class AssetProfileStream(RESTStream):
    """Retrieve a daily snapshot of an asset's core profile.

    It runs once per day per token to capture unique qualitative,
    social, developer, and score metrics not available in the historical
    streams.
    """

    name = "asset_profile"
    primary_keys = ["id", "snapshot_date"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_date"
    state_partitioning_keys = ["token"]
    path = "/coins/{token}"

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        api_url = self.config.get("api_url")
        if api_url in [ApiType.PRO.value, ApiType.FREE.value]:
            return api_url
        raise ValueError(f"Invalid `api_url` provided: {api_url}")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed, following the required paradigm."""
        headers = super().http_headers.copy()
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")

        if not header_key:
            raise ValueError(f"Invalid API URL in config: {self.config['api_url']}")
        if self.config["api_url"] == ApiType.PRO.value and not api_key:
            raise ValueError("API key is required for the CoinGecko Pro API.")

        if api_key:
            headers[header_key] = api_key
        return headers

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Request all data panes to ensure we capture every available field."""
        return {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false",
        }

    def get_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Override the default `get_records` to implement the once-per-day logic.

        This method iterates through the configured tokens, checking the stream state
        to see if a sync has already occurred today.
        """
        today_str = pendulum.now("UTC").to_date_string()

        for token_id in self.config.get("token", []):
            token_context = {"token": token_id}
            stream_state = self.get_context_state(token_context)
            last_synced_date = stream_state.get("replication_key_value")

            if last_synced_date and last_synced_date >= today_str:
                self.logger.info(
                    f"Skipping '{token_id}' for stream '{self.name}'. "
                    f"Already synced today ({today_str})."
                )
                continue

            self.logger.info(f"Fetching daily profile snapshot for '{token_id}'.")
            try:
                full_context = {**(context or {}), **token_context}
                yield from super().get_records(full_context)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"Token '{token_id}' not found on CoinGecko. Skipping.")
                else:
                    raise FatalAPIError(f"Fatal HTTP error for '{token_id}': {e}") from e

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the single record from the response."""
        try:
            yield response.json()
        except requests.exceptions.JSONDecodeError as e:
            raise FatalAPIError(f"Error decoding JSON from response: {response.text}") from e

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Transform the raw API response into a comprehensive flattened record.

        Based on current CoinGecko API documentation (2025).
        """
        market_data = row.get("market_data", {}) or {}
        community_data = row.get("community_data", {}) or {}
        developer_data = row.get("developer_data", {}) or {}
        links = row.get("links", {}) or {}
        image = row.get("image", {}) or {}
        platforms = row.get("platforms", {}) or {}
        detail_platforms = row.get("detail_platforms", {}) or {}
        code_additions_deletions = developer_data.get("code_additions_deletions_4_weeks", {}) or {}
        
        # Helper function to safely get USD values from currency objects
        def get_usd_value(currency_obj):
            if isinstance(currency_obj, dict):
                return currency_obj.get("usd")
            return None

        # Helper function to safely get first item from array
        def get_first_array_item(arr):
            if isinstance(arr, list) and len(arr) > 0:
                return arr[0]
            return None

        return {
            # Core identification and timestamp
            "snapshot_date": pendulum.now("UTC").to_date_string(),
            "id": row.get("id"),
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "web_slug": row.get("web_slug"),
            
            # Platform and technical details
            "asset_platform_id": row.get("asset_platform_id"),
            "platforms": platforms,
            "detail_platforms": detail_platforms,
            "block_time_in_minutes": row.get("block_time_in_minutes"),
            "hashing_algorithm": row.get("hashing_algorithm"),
            
            # Categories and metadata
            "categories": row.get("categories"),
            "preview_listing": row.get("preview_listing"),
            "public_notice": row.get("public_notice"),
            "additional_notices": row.get("additional_notices"),
            "description": row.get("description", {}).get("en") if row.get("description") else None,
            "country_origin": row.get("country_origin"),
            "genesis_date": row.get("genesis_date"),
            
            # Images
            "image_thumb": image.get("thumb"),
            "image_small": image.get("small"),
            "image_large": image.get("large"),
            
            # Sentiment and community metrics
            "sentiment_votes_up_percentage": row.get("sentiment_votes_up_percentage"),
            "sentiment_votes_down_percentage": row.get("sentiment_votes_down_percentage"),
            "watchlist_portfolio_users": row.get("watchlist_portfolio_users"),
            
            # Market ranking
            "market_cap_rank": row.get("market_cap_rank"),
            
            # Current market data (USD focus)
            "current_price_usd": get_usd_value(market_data.get("current_price")),
            "market_cap_usd": get_usd_value(market_data.get("market_cap")),
            "fully_diluted_valuation_usd": get_usd_value(market_data.get("fully_diluted_valuation")),
            "total_volume_usd": get_usd_value(market_data.get("total_volume")),
            "high_24h_usd": get_usd_value(market_data.get("high_24h")),
            "low_24h_usd": get_usd_value(market_data.get("low_24h")),
            
            # Price changes (percentage)
            "price_change_24h": market_data.get("price_change_24h"),
            "price_change_percentage_24h": market_data.get("price_change_percentage_24h"),
            "price_change_percentage_7d": market_data.get("price_change_percentage_7d"),
            "price_change_percentage_14d": market_data.get("price_change_percentage_14d"),
            "price_change_percentage_30d": market_data.get("price_change_percentage_30d"),
            "price_change_percentage_60d": market_data.get("price_change_percentage_60d"),
            "price_change_percentage_200d": market_data.get("price_change_percentage_200d"),
            "price_change_percentage_1y": market_data.get("price_change_percentage_1y"),
            
            # Price changes in currency (USD)
            "price_change_percentage_1h_usd": get_usd_value(market_data.get("price_change_percentage_1h_in_currency")),
            "price_change_percentage_24h_usd": get_usd_value(market_data.get("price_change_percentage_24h_in_currency")),
            "price_change_percentage_7d_usd": get_usd_value(market_data.get("price_change_percentage_7d_in_currency")),
            "price_change_percentage_14d_usd": get_usd_value(market_data.get("price_change_percentage_14d_in_currency")),
            "price_change_percentage_30d_usd": get_usd_value(market_data.get("price_change_percentage_30d_in_currency")),
            "price_change_percentage_60d_usd": get_usd_value(market_data.get("price_change_percentage_60d_in_currency")),
            "price_change_percentage_200d_usd": get_usd_value(market_data.get("price_change_percentage_200d_in_currency")),
            "price_change_percentage_1y_usd": get_usd_value(market_data.get("price_change_percentage_1y_in_currency")),
            
            # Market cap changes
            "market_cap_change_24h": market_data.get("market_cap_change_24h"),
            "market_cap_change_percentage_24h": market_data.get("market_cap_change_percentage_24h"),
            "market_cap_change_24h_usd": get_usd_value(market_data.get("market_cap_change_24h_in_currency")),
            "market_cap_change_percentage_24h_usd": get_usd_value(market_data.get("market_cap_change_percentage_24h_in_currency")),
            
            # Supply metrics
            "total_supply": market_data.get("total_supply"),
            "max_supply": market_data.get("max_supply"),
            "circulating_supply": market_data.get("circulating_supply"),
            
            # All-time high (ATH)
            "ath_usd": get_usd_value(market_data.get("ath")),
            "ath_change_percentage_usd": get_usd_value(market_data.get("ath_change_percentage")),
            "ath_date_usd": get_usd_value(market_data.get("ath_date")),
            
            # All-time low (ATL)
            "atl_usd": get_usd_value(market_data.get("atl")),
            "atl_change_percentage_usd": get_usd_value(market_data.get("atl_change_percentage")),
            "atl_date_usd": get_usd_value(market_data.get("atl_date")),
            
            # TVL and DeFi metrics
            "total_value_locked": market_data.get("total_value_locked"),
            "mcap_to_tvl_ratio": market_data.get("mcap_to_tvl_ratio"),
            "fdv_to_tvl_ratio": market_data.get("fdv_to_tvl_ratio"),
            "market_cap_fdv_ratio": market_data.get("market_cap_fdv_ratio"),
            
            # ROI data (extract individual fields from roi object)
            "roi_times": roi_data.get("times") if roi_data else None,
            "roi_currency": roi_data.get("currency") if roi_data else None,
            "roi_percentage": roi_data.get("percentage") if roi_data else None,
            
            # Market data timestamp
            "market_data_last_updated": market_data.get("last_updated"),
            
            # Community data (limited to available fields)
            "facebook_likes": community_data.get("facebook_likes"),
            "reddit_average_posts_48h": community_data.get("reddit_average_posts_48h"),
            "reddit_average_comments_48h": community_data.get("reddit_average_comments_48h"),
            "reddit_subscribers": community_data.get("reddit_subscribers"),
            "reddit_accounts_active_48h": community_data.get("reddit_accounts_active_48h"),
            "telegram_channel_user_count": community_data.get("telegram_channel_user_count"),
            
            # Developer data
            "developer_forks": developer_data.get("forks"),
            "developer_stars": developer_data.get("stars"),
            "developer_subscribers": developer_data.get("subscribers"),
            "developer_total_issues": developer_data.get("total_issues"),
            "developer_closed_issues": developer_data.get("closed_issues"),
            "developer_pull_requests_merged": developer_data.get("pull_requests_merged"),
            "developer_pull_request_contributors": developer_data.get("pull_request_contributors"),
            "developer_commit_count_4_weeks": developer_data.get("commit_count_4_weeks"),
            "developer_code_additions_4_weeks": code_additions_deletions.get("additions"),
            "developer_code_deletions_4_weeks": code_additions_deletions.get("deletions"),
            "developer_last_4_weeks_commit_activity_series": developer_data.get("last_4_weeks_commit_activity_series"),
            
            # Links and social media
            "homepage_url": get_first_array_item(links.get("homepage")),
            "whitepaper_url": get_first_array_item(links.get("whitepaper")),
            "blockchain_site_url": get_first_array_item(links.get("blockchain_site")),
            "official_forum_url": get_first_array_item(links.get("official_forum_url")),
            "chat_url": get_first_array_item(links.get("chat_url")),
            "announcement_url": get_first_array_item(links.get("announcement_url")),
            "snapshot_url": links.get("snapshot_url"),
            "twitter_screen_name": links.get("twitter_screen_name"),
            "facebook_username": links.get("facebook_username"),
            "bitcointalk_thread_identifier": links.get("bitcointalk_thread_identifier"),
            "telegram_channel_identifier": links.get("telegram_channel_identifier"),
            "subreddit_url": links.get("subreddit_url"),
            "repos_url_github": links.get("repos_url", {}).get("github", [None])[0] if links.get("repos_url", {}).get("github") else None,
            
            # Additional metadata
            "status_updates_count": len(row.get("status_updates", [])) if row.get("status_updates") else 0,
            "last_updated": row.get("last_updated"),
        }

    schema = th.PropertiesList(
        # Core identification and timestamp
        th.Property("snapshot_date", th.DateType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("web_slug", th.StringType),
        
        # Platform and technical details
        th.Property("asset_platform_id", th.StringType),
        th.Property("platforms", th.ObjectType()),
        th.Property("detail_platforms", th.ObjectType()),
        th.Property("block_time_in_minutes", th.NumberType),
        th.Property("hashing_algorithm", th.StringType),
        
        # Categories and metadata
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("preview_listing", th.BooleanType),
        th.Property("public_notice", th.StringType),
        th.Property("additional_notices", th.ArrayType(th.StringType)),
        th.Property("description", th.StringType),
        th.Property("country_origin", th.StringType),
        th.Property("genesis_date", th.DateType),
        
        # Images
        th.Property("image_thumb", th.StringType),
        th.Property("image_small", th.StringType),
        th.Property("image_large", th.StringType),
        
        # Sentiment and community metrics
        th.Property("sentiment_votes_up_percentage", th.NumberType),
        th.Property("sentiment_votes_down_percentage", th.NumberType),
        th.Property("watchlist_portfolio_users", th.NumberType),
        
        # Market ranking
        th.Property("market_cap_rank", th.NumberType),
        
        # Current market data (USD focus)
        th.Property("current_price_usd", th.NumberType),
        th.Property("market_cap_usd", th.NumberType),
        th.Property("fully_diluted_valuation_usd", th.NumberType),
        th.Property("total_volume_usd", th.NumberType),
        th.Property("high_24h_usd", th.NumberType),
        th.Property("low_24h_usd", th.NumberType),
        
        # Price changes (percentage)
        th.Property("price_change_24h", th.NumberType),
        th.Property("price_change_percentage_24h", th.NumberType),
        th.Property("price_change_percentage_7d", th.NumberType),
        th.Property("price_change_percentage_14d", th.NumberType),
        th.Property("price_change_percentage_30d", th.NumberType),
        th.Property("price_change_percentage_60d", th.NumberType),
        th.Property("price_change_percentage_200d", th.NumberType),
        th.Property("price_change_percentage_1y", th.NumberType),
        
        # Price changes in currency (USD)
        th.Property("price_change_percentage_1h_usd", th.NumberType),
        th.Property("price_change_percentage_24h_usd", th.NumberType),
        th.Property("price_change_percentage_7d_usd", th.NumberType),
        th.Property("price_change_percentage_14d_usd", th.NumberType),
        th.Property("price_change_percentage_30d_usd", th.NumberType),
        th.Property("price_change_percentage_60d_usd", th.NumberType),
        th.Property("price_change_percentage_200d_usd", th.NumberType),
        th.Property("price_change_percentage_1y_usd", th.NumberType),
        
        # Market cap changes
        th.Property("market_cap_change_24h", th.NumberType),
        th.Property("market_cap_change_percentage_24h", th.NumberType),
        th.Property("market_cap_change_24h_usd", th.NumberType),
        th.Property("market_cap_change_percentage_24h_usd", th.NumberType),
        
        # Supply metrics
        th.Property("total_supply", th.NumberType),
        th.Property("max_supply", th.NumberType),
        th.Property("circulating_supply", th.NumberType),
        
        # All-time high (ATH)
        th.Property("ath_usd", th.NumberType),
        th.Property("ath_change_percentage_usd", th.NumberType),
        th.Property("ath_date_usd", th.DateType),
        
        # All-time low (ATL)
        th.Property("atl_usd", th.NumberType),
        th.Property("atl_change_percentage_usd", th.NumberType),
        th.Property("atl_date_usd", th.DateType),
        
        # TVL and DeFi metrics
        th.Property("total_value_locked", th.NumberType),
        th.Property("mcap_to_tvl_ratio", th.NumberType),
        th.Property("fdv_to_tvl_ratio", th.NumberType),
        th.Property("market_cap_fdv_ratio", th.NumberType),
        
        # ROI data (individual fields from roi object)
        th.Property("roi_times", th.NumberType),
        th.Property("roi_currency", th.StringType),
        th.Property("roi_percentage", th.NumberType),
        
        # Market data timestamp
        th.Property("market_data_last_updated", th.DateTimeType),
        
        # Community data (current available fields only)
        th.Property("facebook_likes", th.NumberType),
        th.Property("reddit_average_posts_48h", th.NumberType),
        th.Property("reddit_average_comments_48h", th.NumberType),
        th.Property("reddit_subscribers", th.NumberType),
        th.Property("reddit_accounts_active_48h", th.NumberType),
        th.Property("telegram_channel_user_count", th.NumberType),
        
        # Developer data
        th.Property("developer_forks", th.NumberType),
        th.Property("developer_stars", th.NumberType),
        th.Property("developer_subscribers", th.NumberType),
        th.Property("developer_total_issues", th.NumberType),
        th.Property("developer_closed_issues", th.NumberType),
        th.Property("developer_pull_requests_merged", th.NumberType),
        th.Property("developer_pull_request_contributors", th.NumberType),
        th.Property("developer_commit_count_4_weeks", th.NumberType),
        th.Property("developer_code_additions_4_weeks", th.NumberType),
        th.Property("developer_code_deletions_4_weeks", th.NumberType),
        th.Property("developer_last_4_weeks_commit_activity_series", th.ArrayType(th.NumberType)),
        
        # Links and social media
        th.Property("homepage_url", th.StringType),
        th.Property("whitepaper_url", th.StringType),
        th.Property("blockchain_site_url", th.StringType),
        th.Property("official_forum_url", th.StringType),
        th.Property("chat_url", th.StringType),
        th.Property("announcement_url", th.StringType),
        th.Property("snapshot_url", th.StringType),
        th.Property("twitter_screen_name", th.StringType),
        th.Property("facebook_username", th.StringType),
        th.Property("bitcointalk_thread_identifier", th.StringType),
        th.Property("telegram_channel_identifier", th.StringType),
        th.Property("subreddit_url", th.StringType),
        th.Property("repos_url_github", th.StringType),
        
        # Additional metadata
        th.Property("status_updates_count", th.NumberType),
        th.Property("last_updated", th.DateTimeType),
        
    ).to_dict()
    