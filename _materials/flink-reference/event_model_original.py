"""
Event data model for PyFlink stream processing
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, Union
import json


@dataclass
class Event:
    """
    Core event model for all stream processing
    Represents a single user interaction or system event
    """
    user_id: Optional[int] = None
    event_id: Optional[str] = None
    session_id: Optional[str] = None
    event_type: str = "unknown"
    event_name: Optional[str] = None
    properties: Optional[Dict[str, Any]] = field(default_factory=dict)
    url: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    timestamp: Optional[Union[str, datetime]] = None
    date_partition: Optional[str] = None
    
    def __post_init__(self):
        """Post-initialization processing"""
        # Generate event_id if not provided
        if not self.event_id:
            import uuid
            self.event_id = f"evt_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}"
        
        # Convert timestamp string to datetime if needed
        if isinstance(self.timestamp, str):
            try:
                self.timestamp = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                self.timestamp = datetime.utcnow()
        elif self.timestamp is None:
            self.timestamp = datetime.utcnow()
        
        # Generate date partition if not provided
        if not self.date_partition:
            self.date_partition = self.timestamp.strftime('%Y-%m-%d')
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create Event from dictionary (e.g., from Kafka JSON)"""
        return cls(
            user_id=data.get('user_id'),
            event_id=data.get('event_id'),
            session_id=data.get('session_id'),
            event_type=data.get('event_type', 'unknown'),
            event_name=data.get('event_name'),
            properties=data.get('properties', {}),
            url=data.get('url'),
            country=data.get('country'),
            city=data.get('city'),
            timestamp=data.get('timestamp'),
            date_partition=data.get('date_partition')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Event to dictionary for serialization"""
        return {
            'event_id': self.event_id,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'event_type': self.event_type,
            'event_name': self.event_name,
            'properties': self.properties,
            'url': self.url,
            'country': self.country,
            'city': self.city,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'date_partition': self.date_partition
        }
    
    def to_json(self) -> str:
        """Convert Event to JSON string"""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Event':
        """Create Event from JSON string"""
        return cls.from_dict(json.loads(json_str))
    
    # Utility methods for stream processing
    def get_event_time_millis(self) -> int:
        """Get event time as milliseconds timestamp"""
        return int(self.timestamp.timestamp() * 1000)
    
    def get_event_key(self) -> str:
        """Get partitioning key - prefer user_id, fallback to event_id"""
        return str(self.user_id) if self.user_id else self.event_id
    
    def has_user(self) -> bool:
        """Check if event has user information"""
        return self.user_id is not None
    
    def has_session(self) -> bool:
        """Check if event has session information"""
        return self.session_id is not None and self.session_id.strip()
    
    def is_page_view(self) -> bool:
        """Check if this is a page view event"""
        return self.event_type == 'page_view'
    
    def is_purchase(self) -> bool:
        """Check if this is a purchase event"""
        return self.event_type == 'purchase'
    
    def is_error(self) -> bool:
        """Check if this is an error event"""
        return self.event_type == 'error'
    
    def get_property(self, key: str, default=None):
        """Get property value with default"""
        return self.properties.get(key, default) if self.properties else default
    
    def get_amount(self) -> float:
        """Get purchase amount if available"""
        if self.is_purchase() and self.properties:
            amount = self.properties.get('amount', 0)
            return float(amount) if amount else 0.0
        return 0.0
    
    def with_session_id(self, session_id: str) -> 'Event':
        """Create a copy with different session_id"""
        import copy
        new_event = copy.deepcopy(self)
        new_event.session_id = session_id
        return new_event
    
    def with_timestamp(self, timestamp: datetime) -> 'Event':
        """Create a copy with different timestamp"""
        import copy
        new_event = copy.deepcopy(self)
        new_event.timestamp = timestamp
        new_event.date_partition = timestamp.strftime('%Y-%m-%d')
        return new_event
    
    def __str__(self) -> str:
        return f"Event(id={self.event_id}, user={self.user_id}, type={self.event_type}, time={self.timestamp})"
    
    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class UserSession:
    """
    Represents a completed user session with aggregated metrics
    """
    session_id: str
    user_id: int
    start_time: datetime
    end_time: datetime
    duration_seconds: int = 0
    event_count: int = 0
    page_view_count: int = 0
    unique_urls_count: int = 0
    countries: set = field(default_factory=set)
    referrer: Optional[str] = None
    landing_page: Optional[str] = None
    exit_page: Optional[str] = None
    page_view_sequence: list = field(default_factory=list)
    is_bounce: bool = False
    device_type: Optional[str] = None
    browser: Optional[str] = None
    date_partition: Optional[str] = None
    
    def __post_init__(self):
        """Calculate derived fields"""
        if self.start_time and self.end_time:
            self.duration_seconds = int((self.end_time - self.start_time).total_seconds())
        
        if not self.date_partition:
            self.date_partition = self.start_time.strftime('%Y-%m-%d')
        
        # Determine if session is a bounce
        self.is_bounce = self.page_view_count <= 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'session_id': self.session_id,
            'user_id': self.user_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'event_count': self.event_count,
            'page_view_count': self.page_view_count,
            'unique_urls_count': self.unique_urls_count,
            'countries': list(self.countries) if self.countries else [],
            'referrer': self.referrer,
            'landing_page': self.landing_page,
            'exit_page': self.exit_page,
            'page_view_sequence': self.page_view_sequence,
            'is_bounce': self.is_bounce,
            'device_type': self.device_type,
            'browser': self.browser,
            'date_partition': self.date_partition
        }
    
    @classmethod
    def from_events(cls, session_id: str, user_id: int, events: list) -> 'UserSession':
        """Create UserSession from list of events"""
        if not events:
            return None
        
        events = sorted(events, key=lambda e: e.timestamp)
        start_time = events[0].timestamp
        end_time = events[-1].timestamp
        
        page_views = [e for e in events if e.is_page_view()]
        unique_urls = set(e.url for e in page_views if e.url)
        countries = set(e.country for e in events if e.country)
        
        # Extract page view sequence
        page_sequence = [e.url for e in page_views if e.url]
        
        # Determine landing and exit pages
        landing_page = page_sequence[0] if page_sequence else None
        exit_page = page_sequence[-1] if page_sequence else None
        
        # Simple device detection from user agent in properties
        device_info = cls._extract_device_info(events)
        
        return cls(
            session_id=session_id,
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            event_count=len(events),
            page_view_count=len(page_views),
            unique_urls_count=len(unique_urls),
            countries=countries,
            landing_page=landing_page,
            exit_page=exit_page,
            page_view_sequence=page_sequence,
            device_type=device_info.get('device_type'),
            browser=device_info.get('browser')
        )
    
    @staticmethod
    def _extract_device_info(events: list) -> Dict[str, str]:
        """Extract device info from events"""
        for event in events:
            if event.properties and 'user_agent' in event.properties:
                ua = event.properties['user_agent'].lower()
                
                device_type = 'desktop'
                if 'mobile' in ua or 'android' in ua or 'iphone' in ua:
                    device_type = 'mobile'
                elif 'tablet' in ua or 'ipad' in ua:
                    device_type = 'tablet'
                
                browser = 'other'
                if 'chrome' in ua:
                    browser = 'chrome'
                elif 'firefox' in ua:
                    browser = 'firefox'
                elif 'safari' in ua:
                    browser = 'safari'
                elif 'edge' in ua:
                    browser = 'edge'
                
                return {'device_type': device_type, 'browser': browser}
        
        return {'device_type': None, 'browser': None}
    
    def get_duration_minutes(self) -> float:
        """Get session duration in minutes"""
        return self.duration_seconds / 60.0
    
    def is_long_session(self) -> bool:
        """Check if session is longer than 30 minutes"""
        return self.duration_seconds > 1800
    
    def get_primary_country(self) -> Optional[str]:
        """Get the primary country for this session"""
        return list(self.countries)[0] if self.countries else None