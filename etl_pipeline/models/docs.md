# Documentation for Silver Model Layer

{% docs case_id %}
Unique identifier for each 311 service request. This is the primary key that tracks individual service cases from request to completion. Originally named `service_request_id` in the raw data.
{% enddocs %}

{% docs status %}
Current status of the service request. Indicates whether the case is currently open and being addressed, or has been closed and resolved. Valid values are `Open` for active cases and `Closed` for completed cases.
{% enddocs %}

{% docs latitude %}
Geographic latitude coordinate of the service request location. Represents the north-south position in decimal degrees. Values should fall within San Francisco's geographic bounds `(37.73 to 37.83)`.
{% enddocs %}

{% docs longitude %}
Geographic longitude coordinate of the service request location. Represents the east-west position in decimal degrees. Values should fall within San Francisco's geographic bounds `(-122.52 to -122.36)`.
{% enddocs %}

{% docs point_geom %}
PostGIS geometry object representing the precise geographic location of the service request. This spatial data enables advanced geographic analysis, distance calculations, and spatial joins with other geographic datasets using PostGIS functions like `ST_Distance` and `ST_Within`.
{% enddocs %}

{% docs opened %}
Timestamp when the service request was initially submitted by the citizen or reporting party. This marks the start of the service timeline.
{% enddocs %}

{% docs closed %}
Timestamp when the service request was officially completed and closed by the responsible agency. Used to calculate response time metrics.
{% enddocs %}

{% docs updated %}
Timestamp of the most recent update or status change for the service request. Tracks when the case was last modified.
{% enddocs %}

{% docs status_notes %}
Additional details or comments about the current status. May include resolution details, work notes, or explanatory text about case progress.
{% enddocs %}

{% docs responsible_agency %}
City department or agency responsible for addressing the service request. Examples include `DPW` (Department of Public Works), `SFPD` (Police Department), or `SFFD` (Fire Department).
{% enddocs %}

{% docs service_name %}
Primary category of service being requested. High-level classification such as `Graffiti Removal`, `Pothole Repair`, or `Street Light Outage` e.t.c.
{% enddocs %}

{% docs service_subtype %}
More specific subcategory within the service type. Provides additional detail about the nature of the request.
{% enddocs %}

{% docs service_details %}
Free-text description with specific details about the service issue or request. Contains the citizen's original description of the problem.
{% enddocs %}

{% docs address %}
Full street address where the service is needed. Includes street number, street name, and sometimes cross streets or location details.
{% enddocs %}

{% docs street %}
Street name only (without number) where the service is located. Used for geographic analysis and neighborhood mapping.
{% enddocs %}

{% docs supervisor_district %}
San Francisco Board of Supervisors district number where the service request is located. Districts are numbered 1-11 and represent political boundaries.
{% enddocs %}

{% docs neighborhood %}
Geographic neighborhood designation using the SF Find neighborhoods boundaries. Official neighborhood classification for spatial analysis.
{% enddocs %}

{% docs analysis_neighborhood %}
Alternative neighborhood classification used for city planning and demographic analysis. May differ slightly from standard neighborhood boundaries.
{% enddocs %}

{% docs police_district %}
San Francisco Police Department district where the service request is located. Used for public safety and law enforcement analysis.
{% enddocs %}

{% docs source_channel %}
Method through which the service request was submitted. Examples include mobile app, web portal, phone call, or in-person report.
{% enddocs %}

{% docs media_url %}
URL link to any photos, documents, or media files attached to the service request. Often contains visual evidence of the issue reported.
{% enddocs %}

{% docs supervisor_district_2012 %}
Supervisor district assignment based on 2012 district boundaries. Used for historical comparison and longitudinal analysis across redistricting periods.
{% enddocs %}

{% docs data_as_of %}
Timestamp indicating when this data record was extracted or updated in the source system. Used for data freshness tracking and incremental processing.
{% enddocs %}

{% docs data_loaded_at %}
Timestamp when this record was loaded into the data warehouse. Tracks ETL pipeline execution and data ingestion timing.
{% enddocs %}

{% docs response_time_hours %}
Calculated metric representing the time between request submission and case closure. Measured in hours, this indicates service efficiency and responsiveness.
{% enddocs %}
