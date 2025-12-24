# NFL Salary Cap - Authoritative Sources

## Official NFL Sources (Most Authoritative)

### 1. **NFL Communications / NFL.com**
- **URL**: https://communications.nfl.com/ and https://operations.nfl.com/
- **Reliability**: ⭐⭐⭐⭐⭐ (Primary source)
- **Data Available**: Annual salary cap announcements, CBA terms, benefit pools
- **Example**: "2024 salary cap set at $255.4 million"
- **Access**: Press releases, public announcements
- **Pros**: Official, authoritative, legally binding
- **Cons**: Not always in structured format, manual parsing required

### 2. **NFLPA (NFL Players Association)**
- **URL**: https://nflpa.com/ and https://nflpa.com/resources/public-salary-cap-report
- **Reliability**: ⭐⭐⭐⭐⭐ (Co-official source)
- **Data Available**: Salary cap reports, player contract details, team cap space
- **Pros**: Player-side official source, sometimes more transparent
- **Cons**: May require login, data format varies

## Secondary Sources (High Quality)

### 3. **Spotrac** ⭐ CURRENT PRIMARY SOURCE
- **URL**: https://www.spotrac.com/nfl/cap/
- **Reliability**: ⭐⭐⭐⭐ (Aggregator, widely trusted)
- **Data Available**: 
  - Team cap space by year
  - Player contracts
  - Dead money breakdowns
  - Historical cap data back to 2011
- **Current Use**: Already scraping team/player data
- **Known Issues**:
  - Active cap + dead money ≠ total cap (our data shows discrepancy)
  - May not reflect real-time adjustments
  - Subject to corrections/updates

### 4. **Over The Cap**
- **URL**: https://overthecap.com/
- **Reliability**: ⭐⭐⭐⭐⭐ (Industry standard, used by analysts)
- **Data Available**:
  - Real-time salary cap tracking
  - Player contracts with full breakdowns
  - Dead money calculations
  - Contract restructures
  - Historical data
- **Pros**: 
  - Extremely detailed
  - API-friendly structure (HTML tables)
  - Updated daily during season
  - Used by NFL media/analysts
- **Cons**: No official API

### 5. **Pro Football Reference (PFR)**
- **URL**: https://www.pro-football-reference.com/
- **Reliability**: ⭐⭐⭐⭐ (Comprehensive stats database)
- **Data Available**:
  - Historical salary cap data
  - Player salaries
  - Team payroll summaries
- **Pros**: Clean data structure, historical depth
- **Cons**: Less real-time than OTC/Spotrac

## Reference Data: Historical NFL Salary Caps

| Year | Salary Cap (per team) | Player Benefits | Total Cost | Source |
|------|----------------------|-----------------|------------|--------|
| 2024 | $255,400,000 | $74,000,000 | $329,400,000 | NFL.com |
| 2023 | $224,800,000 | $68,600,000 | $293,400,000 | NFL.com |
| 2022 | $208,200,000 | $64,000,000 | $272,200,000 | NFL.com |
| 2021 | $182,500,000 | $58,000,000 | $240,500,000 | NFL.com |
| 2020 | $198,200,000 | $61,000,000 | $259,200,000 | NFL.com |
| 2019 | $188,200,000 | $59,000,000 | $247,200,000 | NFL.com |
| 2018 | $177,200,000 | $55,000,000 | $232,200,000 | NFL.com |
| 2017 | $167,000,000 | $52,000,000 | $219,000,000 | NFL.com |
| 2016 | $155,270,000 | $48,000,000 | $203,270,000 | NFL.com |
| 2015 | $143,280,000 | $45,000,000 | $188,280,000 | NFL.com |

**32 teams × salary cap = total league cap**

## Implementation Strategy

### Option 1: Hardcode Reference Data (Recommended for Now)
Create a Python module with official cap figures:

```python
# src/salary_cap_reference.py
NFL_SALARY_CAPS = {
    2024: 255.4,  # millions
    2023: 224.8,
    2022: 208.2,
    2021: 182.5,
    2020: 198.2,
    2019: 188.2,
    2018: 177.2,
    2017: 167.0,
    2016: 155.27,
    2015: 143.28,
}
```

**Pros**: Simple, fast, reliable, no scraping
**Cons**: Manual updates annually

### Option 2: Scrape from Over The Cap
```python
# URL: https://overthecap.com/salary-cap-space
# Parse historical cap table
# Store as reference data
```

**Pros**: Automated, comprehensive, historical
**Cons**: Adds scraping dependency

### Option 3: Use Spotrac's Cap Page
```python
# URL: https://www.spotrac.com/nfl/cap/
# Extract "League Cap: $255.4M" from page header
```

**Pros**: Single source consistency
**Cons**: Already using Spotrac, potential circular validation

## Recommended Approach

**Phase 1 (Immediate)**: Hardcode reference caps from NFL.com announcements
- Create `src/salary_cap_reference.py` with official figures
- Add validation tests comparing scraped totals to reference
- Set tolerance thresholds (e.g., ±5% for team totals)

**Phase 2 (Future)**: Integrate Over The Cap
- Scrape OTC as secondary validation source
- Cross-reference Spotrac vs OTC vs NFL official
- Flag discrepancies for manual review

## Validation Tests Needed

### Test 1: League-Wide Cap Total
```python
def test_league_total_cap(year):
    """Sum of all 32 team caps should equal 32 × official cap"""
    expected = NFL_SALARY_CAPS[year] * 32
    actual = sum(team_caps_for_year)
    assert abs(actual - expected) / expected < 0.05  # 5% tolerance
```

### Test 2: Individual Team Cap Reasonableness
```python
def test_team_cap_within_range(year):
    """Each team should be near the official cap (accounting for rollover/adjustments)"""
    official_cap = NFL_SALARY_CAPS[year]
    for team_cap in team_caps:
        # Teams can exceed cap due to carryover, adjustments
        assert 0.8 * official_cap <= team_cap <= 1.15 * official_cap
```

### Test 3: Components Sum Correctly
```python
def test_cap_components_sum():
    """Active cap + dead money ≈ total cap (within accounting tolerance)"""
    for team in teams:
        calculated = team.active_cap + team.dead_money
        assert abs(calculated - team.total_cap) < 1.0  # $1M tolerance
```

### Test 4: Historical Cap Progression
```python
def test_cap_increases_over_time():
    """Cap should generally increase year-over-year (except 2021 COVID)"""
    for year in range(2016, 2024):
        if year != 2021:  # COVID exception
            assert NFL_SALARY_CAPS[year+1] > NFL_SALARY_CAPS[year]
```

## Current Data Quality Issues

Based on initial analysis:

1. **$201.8M discrepancy** between expected ($8,172.8M) and actual ($7,971.02M) for 2024
   - Possible causes:
     * Incomplete scrape (missing teams?)
     * Mid-season data vs final cap
     * Accounting differences (cash vs cap)
     * Cap adjustments not yet applied

2. **Wide team cap variation**: $226.1M to $261.5M
   - Expected: Some variation due to rollover credits
   - Needs investigation if range exceeds ±10%

3. **Components don't sum**: Active + Dead ≠ Total
   - May be Spotrac calculation methodology
   - Need to understand their accounting

## Action Items

1. ✅ Document authoritative sources (this file)
2. ⬜ Create `salary_cap_reference.py` with official caps
3. ⬜ Add salary cap validation tests to test suite
4. ⬜ Investigate $201M discrepancy in 2024 data
5. ⬜ Consider adding Over The Cap as secondary source
6. ⬜ Set up automated cap updates (annual task)

## References

- NFL Operations: https://operations.nfl.com/
- NFLPA Public Cap Reports: https://nflpa.com/resources/public-salary-cap-report
- Spotrac NFL Cap: https://www.spotrac.com/nfl/cap/
- Over The Cap: https://overthecap.com/salary-cap-space
- Pro Football Reference: https://www.pro-football-reference.com/
