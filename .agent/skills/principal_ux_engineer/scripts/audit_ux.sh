#!/bin/bash

# Principal UX Engineer Audit Script
# Checks for common UX/Accessibility anti-patterns and ensures best practices.

echo "🔍 Starting Principal UX Audit..."
echo "-----------------------------------"

# 1. Check for '100vh' usage (should be 100dvh for mobile)
echo "1. Checking for '100vh' anti-pattern (Mobile Viewport Bug)..."
grep -r "100vh" . --include="*.css" --include="*.tsx" --include="*.jsx" || echo "✅ No '100vh' found (Good)."

# 2. Check for 'outline: none' (Accessibility Sin)
echo "2. Checking for 'outline: none' (Accessibility Violation)..."
grep -r "outline: none" . --include="*.css" --include="*.tsx" --include="*.jsx" || echo "✅ No 'outline: none' found (Good)."

# 3. Check for 'img' tags without 'alt' (Accessibility Sin)
# Simple check, not perfect parser
echo "3. Checking for <img> tags without alt attributes..."
grep -r "<img" . --include="*.tsx" --include="*.jsx" | grep -v "alt=" || echo "✅ Most img tags seem to have alt attributes (Manual check recommended)."

# 4. Check for 'button' without 'type' (Form submission bugs)
echo "4. Checking for <button> without type attribute..."
grep -r "<button" . --include="*.tsx" --include="*.jsx" | grep -v "type=" || echo "✅ Most buttons seem to have type attributes."

echo "-----------------------------------"
echo "🎉 Audit Complete. Fix any violations above."
