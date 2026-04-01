import { google } from 'googleapis';
import { storage } from './storage';

const SCOPES = [
  'https://www.googleapis.com/auth/calendar.events',
  'https://www.googleapis.com/auth/calendar.readonly'
];

// Load saved credentials from database
async function loadSavedCredentialsIfExist(workspaceId: number) {
  try {
    const tokenData = await storage.getGoogleCalendarToken(workspaceId);
    if (!tokenData) {
      return null;
    }

    const clientId = process.env.GOOGLE_CLIENT_ID!;
    const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
    
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
    oauth2Client.setCredentials({
      access_token: tokenData.accessToken,
      refresh_token: tokenData.refreshToken,
      expiry_date: tokenData.expiryDate ? new Date(tokenData.expiryDate).getTime() : undefined,
    });
    
    return oauth2Client;
  } catch (err) {
    console.error('Error loading calendar credentials from database:', err);
    return null;
  }
}

// Save credentials to database
async function saveCredentials(client: any, workspaceId: number) {
  try {
    const expiryDate = client.credentials.expiry_date 
      ? new Date(client.credentials.expiry_date) 
      : null;

    await storage.saveGoogleCalendarToken({
      workspaceId,
      accessToken: client.credentials.access_token,
      refreshToken: client.credentials.refresh_token,
      expiryDate,
    });
    
    console.log('Google Calendar credentials saved to database. Workspace:', workspaceId, 'Refresh token:', client.credentials.refresh_token ? 'Present' : 'Missing');
  } catch (err) {
    console.error('Error saving credentials to database:', err);
  }
}

// Get calendar events for a job by searching for job ID
export async function getJobCalendarEvents(jobId: string, workspaceId: number = 1): Promise<Array<{date: string, startTime: string, endTime: string, summary: string}>> {
  try {
    const client = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth: client });

    // Search for events containing the job ID
    const events = await calendar.events.list({
      calendarId: 'primary',
      q: jobId,
      maxResults: 10,
      singleEvents: true,
      orderBy: 'startTime',
      timeMin: (new Date()).toISOString(),
    });

    const jobEvents = [];
    if (events.data.items) {
      for (const event of events.data.items) {
        if (event.summary?.includes(jobId) && event.start?.dateTime && event.end?.dateTime) {
          const startDate = new Date(event.start.dateTime);
          const endDate = new Date(event.end.dateTime);
          
          jobEvents.push({
            date: startDate.toISOString().split('T')[0], // YYYY-MM-DD format
            startTime: startDate.toLocaleTimeString('en-US', { 
              hour: 'numeric', 
              minute: '2-digit', 
              hour12: true 
            }),
            endTime: endDate.toLocaleTimeString('en-US', { 
              hour: 'numeric', 
              minute: '2-digit', 
              hour12: true 
            }),
            summary: event.summary
          });
        }
      }
    }

    return jobEvents.sort((a, b) => a.date.localeCompare(b.date));
  } catch (error) {
    console.error('Error fetching job calendar events:', error);
    return [];
  }
}

// Get authorized client (requires workspaceId for database lookup)
async function getAuthClient(workspaceId: number = 1) {
  let client = await loadSavedCredentialsIfExist(workspaceId);
  if (client) {
    // Test the token to make sure it's still valid
    try {
      const calendar = google.calendar({ version: 'v3', auth: client });
      await calendar.calendarList.list({ maxResults: 1 });
      return client;
    } catch (error: any) {
      if (error.message && error.message.includes('invalid_grant')) {
        // Token has expired, remove it from database
        try {
          await storage.deleteGoogleCalendarToken(workspaceId);
          console.log('Removed expired Google Calendar token for workspace:', workspaceId);
        } catch (deleteError) {
          console.log('Error removing expired token:', deleteError);
        }
      }
      throw new Error('Google Calendar token expired. Please reconnect your calendar.');
    }
  }
  
  // If no saved credentials, need manual OAuth flow
  throw new Error('Google Calendar not authorized. Please run authorization flow.');
}

// Create calendar event
export async function createCalendarEvent(eventData: {
  summary: string;
  description?: string;
  location?: string;
  startDateTime: string;
  endDateTime: string;
  timeZone?: string;
}, workspaceId: number = 1) {
  try {
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    const event = {
      summary: eventData.summary,
      location: eventData.location,
      description: eventData.description,
      start: {
        dateTime: eventData.startDateTime,
        timeZone: eventData.timeZone || 'Australia/Perth',
      },
      end: {
        dateTime: eventData.endDateTime,
        timeZone: eventData.timeZone || 'Australia/Perth',
      },
      reminders: {
        useDefault: false,
        overrides: [
          { method: 'email', minutes: 24 * 60 }, // 24 hours
          { method: 'popup', minutes: 30 }, // 30 minutes
        ],
      },
    };
    
    const response = await calendar.events.insert({
      calendarId: 'primary',
      requestBody: event,
    });
    
    return {
      success: true,
      eventId: response.data.id,
      eventUrl: response.data.htmlLink,
      event: response.data
    };
    
  } catch (error) {
    console.error('Error creating calendar event:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

// Update calendar event
export async function updateCalendarEvent(eventId: string, eventData: {
  summary?: string;
  description?: string;
  location?: string;
  startDateTime?: string;
  endDateTime?: string;
  timeZone?: string;
}, workspaceId: number = 1) {
  try {
    console.log(`[Google Calendar] Attempting to update event ${eventId}`);
    console.log(`[Google Calendar] Update data:`, eventData);
    
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    // Get existing event first
    console.log(`[Google Calendar] Fetching existing event...`);
    const existingEvent = await calendar.events.get({
      calendarId: 'primary',
      eventId: eventId,
    });
    
    console.log(`[Google Calendar] Current event times:`, {
      start: existingEvent.data.start,
      end: existingEvent.data.end
    });
    
    // Build the update object with only the fields we want to change
    const updateData: any = {};
    
    if (eventData.summary) updateData.summary = eventData.summary;
    if (eventData.location) updateData.location = eventData.location;
    if (eventData.description) updateData.description = eventData.description;
    
    if (eventData.startDateTime) {
      updateData.start = {
        dateTime: eventData.startDateTime,
        timeZone: eventData.timeZone || 'Australia/Perth',
      };
    }
    
    if (eventData.endDateTime) {
      updateData.end = {
        dateTime: eventData.endDateTime,
        timeZone: eventData.timeZone || 'Australia/Perth',
      };
    }
    
    console.log(`[Google Calendar] Sending update with data:`, updateData);
    
    const response = await calendar.events.update({
      calendarId: 'primary',
      eventId: eventId,
      requestBody: updateData,
    });
    
    console.log(`[Google Calendar] Update response status:`, response.status);
    console.log(`[Google Calendar] Updated event times:`, {
      start: response.data.start,
      end: response.data.end
    });
    
    // Verify the update by fetching the event again after a brief delay
    setTimeout(async () => {
      try {
        const verifyEvent = await calendar.events.get({
          calendarId: 'primary',
          eventId: eventId,
        });
        console.log(`[Google Calendar] Verification - Event times after update:`, {
          start: verifyEvent.data.start,
          end: verifyEvent.data.end,
          htmlLink: verifyEvent.data.htmlLink
        });
      } catch (error) {
        console.error('[Google Calendar] Error verifying event update:', error);
      }
    }, 2000);
    
    return {
      success: true,
      eventId: response.data.id!,
      eventUrl: response.data.htmlLink!,
      event: response.data
    };
    
  } catch (error) {
    console.error('[Google Calendar] Error updating calendar event:', error);
    if (error && typeof error === 'object' && 'message' in error) {
      console.error('[Google Calendar] Error details:', (error as any).message);
    }
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

// Delete calendar event
export async function deleteCalendarEvent(eventId: string, workspaceId: number = 1) {
  try {
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    await calendar.events.delete({
      calendarId: 'primary',
      eventId: eventId,
    });
    
    return {
      success: true,
      message: 'Event deleted successfully'
    };
    
  } catch (error) {
    console.error('Error deleting calendar event:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

// Check if Google Calendar is authorized
export async function isCalendarAuthorized(workspaceId: number = 1): Promise<boolean> {
  try {
    const client = await loadSavedCredentialsIfExist(workspaceId);
    if (!client) return false;
    
    // Actually test the token by making a real API call
    try {
      const calendar = google.calendar({ version: 'v3', auth: client });
      await calendar.calendarList.list({ maxResults: 1 });
      return true;
    } catch (error: any) {
      console.log('Google Calendar API test failed:', error.message);
      if (error.message && error.message.includes('invalid_grant')) {
        // Token has expired, remove it from database
        try {
          await storage.deleteGoogleCalendarToken(workspaceId);
          console.log('Removed expired Google Calendar token for workspace:', workspaceId);
        } catch (deleteError) {
          console.log('Error removing expired token:', deleteError);
        }
      }
      return false;
    }
  } catch (error) {
    console.log('Calendar authorization check failed:', error);
    return false;
  }
}

// Get authorization URL for OAuth flow
export function getAuthUrl(origin?: string): string {
  try {
    const clientId = process.env.GOOGLE_CLIENT_ID!;
    const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
    
    // Use the origin from the request, or fall back to environment detection
    let redirectUri: string;
    if (origin) {
      redirectUri = `${origin}/auth/google/callback`;
      console.log('[Google Calendar] Using redirect URI from request origin:', redirectUri);
    } else {
      // Fallback to environment detection
      const deployedDomain = 'spark-production-4be1.up.railway.app';
      const devDomain = process.env.REPLIT_DOMAINS?.split(',')[0];
      
      const possibleDomains = [
        deployedDomain,
        devDomain,
        'paperworkpro--spark-window-furnishing-meas.replit.app',
        'localhost:5000'
      ].filter(Boolean);
      
      redirectUri = `https://${possibleDomains[0]}/auth/google/callback`;
      console.log('[Google Calendar] Using redirect URI from environment:', redirectUri);
      console.log('[Google Calendar] All possible domains:', possibleDomains);
    }
    
    console.log('');
    console.log('*** IMPORTANT: Add this redirect URI to your Google Cloud Console ***');
    console.log(`- ${redirectUri}`);
    console.log('');
    
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    
    return oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
      prompt: 'select_account consent',
      include_granted_scopes: true,
      hd: undefined,
      login_hint: undefined,
      state: Math.random().toString(36).substring(2)
    });
  } catch (error) {
    console.error('Error generating auth URL:', error);
    throw new Error('Google Calendar credentials not configured');
  }
}

// Handle OAuth callback
export async function handleAuthCallback(code: string, origin?: string) {
  try {
    const clientId = process.env.GOOGLE_CLIENT_ID!;
    const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
    
    // Use the origin from the request, or fall back to environment detection
    let redirectUri: string;
    if (origin) {
      redirectUri = `${origin}/auth/google/callback`;
    } else {
      // Fallback to environment detection
      const deployedDomain = 'spark-production-4be1.up.railway.app';
      const devDomain = process.env.REPLIT_DOMAINS?.split(',')[0];
      
      const possibleDomains = [
        deployedDomain,
        devDomain,
        'paperworkpro--spark-window-furnishing-meas.replit.app',
        'localhost:5000'
      ].filter(Boolean);
      
      redirectUri = `https://${possibleDomains[0]}/auth/google/callback`;
    }
    
    console.log('[Google Calendar] Processing callback with redirect URI:', redirectUri);
    
    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    
    // Save credentials - defaulting to workspace 1 for now
    // In production, this should be retrieved from the user session
    await saveCredentials(oauth2Client, 1);
    
    // Start the sync service after successful authorization
    startCalendarSyncService();
    
    return {
      success: true,
      message: 'Google Calendar authorized successfully'
    };
    
  } catch (error) {
    console.error('Error handling auth callback:', error);
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

// Two-way calendar synchronization service
let syncInterval: NodeJS.Timeout | null = null;

export function startCalendarSyncService() {
  if (syncInterval) {
    clearInterval(syncInterval);
  }
  
  // Start syncing every 5 minutes
  syncInterval = setInterval(async () => {
    try {
      await syncCalendarEvents();
    } catch (error) {
      console.error('Calendar sync error:', error);
    }
  }, 5 * 60 * 1000); // 5 minutes
  
  console.log('Google Calendar sync service started - checking for changes every 5 minutes');
}

export function stopCalendarSyncService() {
  if (syncInterval) {
    clearInterval(syncInterval);
    syncInterval = null;
    console.log('Google Calendar sync service stopped');
  }
}

// Sync calendar events with job database
export async function syncCalendarEvents(workspaceId: number = 1) {
  try {
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    // Get all events created by our app in the next 30 days
    const now = new Date();
    const thirtyDaysFromNow = new Date();
    thirtyDaysFromNow.setDate(now.getDate() + 30);
    
    const response = await calendar.events.list({
      calendarId: 'primary',
      timeMin: now.toISOString(),
      timeMax: thirtyDaysFromNow.toISOString(),
      q: 'FITTER PRO', // Search for events containing our app identifier
      singleEvents: true,
      orderBy: 'startTime',
    });
    
    const events = response.data.items || [];
    console.log(`Found ${events.length} FITTER PRO calendar events to sync`);
    
    // Import storage to access job database
    const { DatabaseStorage } = await import('./storage');
    const storage = new DatabaseStorage();
    
    for (const event of events) {
      if (!event.id || !event.summary) continue;
      
      // Find job by calendar event ID
      const existingJob = await storage.getJobByCalendarEventId(event.id, workspaceId);
      
      if (existingJob) {
        // Update existing job based on calendar event changes
        await syncExistingJob(existingJob, event, workspaceId);
      } else {
        // This is a calendar event that might need to create a new job
        await syncNewCalendarEvent(event);
      }
    }
    
    // Check for deleted calendar events
    await syncDeletedEvents(workspaceId);
    
  } catch (error: any) {
    if (error.message && error.message.includes('token expired')) {
      console.log('Google Calendar sync paused - token expired. Please reconnect your calendar.');
      stopCalendarSyncService(); // Stop the sync service until user reconnects
    } else {
      console.error('Error syncing calendar events:', error);
    }
  }
}

// Sync existing job with calendar event changes
async function syncExistingJob(job: any, event: any, workspaceId: number) {
  try {
    const { DatabaseStorage } = await import('./storage');
    const storage = new DatabaseStorage();
    
    // Extract date from calendar event
    const eventDate = event.start?.date || event.start?.dateTime;
    if (!eventDate) return;
    
    const calendarDate = new Date(eventDate).toISOString().split('T')[0];
    const currentBookingDate = job.bookingDate;
    
    // Check if the booking date has changed in calendar
    if (currentBookingDate !== calendarDate) {
      console.log(`Syncing job ${job.jobId}: booking date changed from ${currentBookingDate} to ${calendarDate}`);
      
      await storage.updateJobBookingDate(job.id, calendarDate, workspaceId);
    }
    
    // Check if event was cancelled/deleted (if event status is cancelled)
    if (event.status === 'cancelled') {
      console.log(`Syncing job ${job.jobId}: calendar event was cancelled, updating status to To Do`);
      
      await storage.updateJobStatus(job.id, 'To Do', workspaceId);
      await storage.updateJobBookingDate(job.id, null, workspaceId);
      await storage.updateJobCalendarEventId(job.id, null, workspaceId);
    }
    
  } catch (error) {
    console.error('Error syncing existing job:', error);
  }
}

// Handle new calendar events that might represent manual bookings
async function syncNewCalendarEvent(event: any) {
  try {
    // For now, we won't auto-create jobs from manual calendar events
    // This prevents unwanted job creation from unrelated calendar events
    // We only sync existing jobs that we know about
    console.log(`Found calendar event without matching job: ${event.summary}`);
    
  } catch (error) {
    console.error('Error syncing new calendar event:', error);
  }
}

// Fix all existing calendar events with correct time slots
export async function fixExistingCalendarEventTimes(workspaceId: number = 1) {
  try {
    const { DatabaseStorage } = await import('./storage');
    const storage = new DatabaseStorage();
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    // Get all jobs with calendar events and booking dates
    const jobsWithEvents = await storage.getJobsWithCalendarEvents(workspaceId);
    console.log(`Found ${jobsWithEvents.length} jobs with calendar events to potentially fix`);
    
    let fixedCount = 0;
    
    for (const job of jobsWithEvents) {
      if (!job.calendarEventId || !job.bookingDate) continue;
      
      try {
        // First check if the calendar event still exists (don't recreate deleted events)
        await calendar.events.get({
          calendarId: 'primary',
          eventId: job.calendarEventId,
        });
        
        // Event exists, proceed with fixing
        const startTime = job.startTime || '09:00';
        const endTime = job.endTime || '17:00';
        
        console.log(`Fixing calendar event for job ${job.jobId}: ${job.bookingDate} ${startTime}-${endTime}`);
        
        // Create datetime strings in Perth timezone
        const startDateTime = `${job.bookingDate}T${startTime}:00`;
        const endDateTime = `${job.bookingDate}T${endTime}:00`;
        const costInfo = (job as any).installCost != null ? `\nCost: $${parseFloat((job as any).installCost).toFixed(2)}` : '';
        
        const updateResult = await updateCalendarEvent(job.calendarEventId, {
          summary: `FITTER PRO - ${job.type}: ${job.clientName}`,
          description: `Job ID: ${job.jobId}\nClient: ${job.clientName}\nType: ${job.type}${costInfo}\n\nScheduled via FITTER PRO`,
          location: job.address,
          startDateTime,
          endDateTime,
          timeZone: 'Australia/Perth'
        }, workspaceId);
        
        if (updateResult.success) {
          fixedCount++;
          console.log(`✓ Fixed calendar event for job ${job.jobId}`);
        } else {
          console.log(`✗ Failed to fix calendar event for job ${job.jobId}: ${updateResult.error}`);
        }
        
      } catch (error: any) {
        if (error.status === 404) {
          // Calendar event was manually deleted, clean up job data
          console.log(`Job ${job.jobId}: calendar event was manually deleted, clearing job booking data`);
          
          await storage.updateJobStatus(job.id, 'To Do', workspaceId);
          await storage.updateJobBookingDate(job.id, null, workspaceId);
          await storage.updateJobCalendarEventId(job.id, null, workspaceId);
        } else {
          console.error(`Error fixing calendar event for job ${job.jobId}:`, error);
        }
      }
    }
    
    console.log(`Fixed ${fixedCount} calendar events with correct time slots`);
    return { success: true, fixedCount };
    
  } catch (error) {
    console.error('Error fixing existing calendar event times:', error);
    return { success: false, error: error instanceof Error ? error.message : 'Unknown error' };
  }
}

// Get daily availability based on ALL calendar events (personal appointments, bookings, etc.)
// Returns busy minutes per day for the next N days
export interface DailyAvailability {
  date: string; // YYYY-MM-DD
  dayName: string; // Mon, Tue, etc.
  busyMinutes: number;
  availableMinutes: number;
  totalWorkMinutes: number; // Default 480 (8 hours)
  events: Array<{ summary: string; startTime: string; endTime: string; duration: number }>;
  capacityPercent: number; // 0-100, how much of the day is still available
}

// Helper to format date in Perth timezone (YYYY-MM-DD)
function formatDatePerth(date: Date): string {
  return date.toLocaleDateString('en-CA', { timeZone: 'Australia/Perth' }); // en-CA gives YYYY-MM-DD format
}

// Helper to get day name in Perth timezone
function getDayNamePerth(date: Date): string {
  return date.toLocaleDateString('en-AU', { weekday: 'short', timeZone: 'Australia/Perth' });
}

// Helper to get day of week in Perth timezone (0 = Sunday)
function getDayOfWeekPerth(date: Date): number {
  const perthDateStr = date.toLocaleDateString('en-US', { weekday: 'long', timeZone: 'Australia/Perth' });
  const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  return days.indexOf(perthDateStr);
}

export async function getCalendarAvailability(
  daysAhead: number = 30,
  workspaceId: number = 1
): Promise<DailyAvailability[]> {
  const WORK_START_HOUR = 7; // 7 AM Perth time
  const WORK_END_HOUR = 17; // 5 PM Perth time 
  const WORK_MINUTES_PER_DAY = (WORK_END_HOUR - WORK_START_HOUR) * 60; // 600 minutes (10 hours)
  
  const availability: DailyAvailability[] = [];
  
  try {
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    const now = new Date();
    const endDate = new Date();
    endDate.setDate(now.getDate() + daysAhead);
    
    const response = await calendar.events.list({
      calendarId: 'primary',
      timeMin: now.toISOString(),
      timeMax: endDate.toISOString(),
      singleEvents: true,
      orderBy: 'startTime',
      maxResults: 500,
      timeZone: 'Australia/Perth', // Request events in Perth timezone
    });
    
    const events = response.data.items || [];
    console.log(`[Calendar Availability] Found ${events.length} calendar events in next ${daysAhead} days`);
    
    // Group events by Perth date - handle multi-day events by expanding to all affected days
    type DayEvent = { summary: string; startMins: number; endMins: number; isAllDay: boolean };
    const eventsByDate: Record<string, DayEvent[]> = {};
    
    const workStartMins = WORK_START_HOUR * 60;
    const workEndMins = WORK_END_HOUR * 60;
    
    const formatMins = (mins: number) => {
      const h = Math.floor(mins / 60);
      const m = mins % 60;
      return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
    };
    
    // Helper to add event to a specific date
    const addEventToDate = (dateKey: string, evt: DayEvent) => {
      if (!eventsByDate[dateKey]) {
        eventsByDate[dateKey] = [];
      }
      eventsByDate[dateKey].push(evt);
    };
    
    for (const event of events) {
      if (!event.start) continue;
      
      // Detect all-day events (they have .date but not .dateTime)
      const isAllDay = !event.start.dateTime && !!event.start.date;
      const startStr = event.start.dateTime || event.start.date;
      const endStr = event.end?.dateTime || event.end?.date;
      if (!startStr || !endStr) continue;
      
      const start = new Date(startStr);
      const end = new Date(endStr);
      const summary = event.summary || 'Busy';
      
      if (isAllDay) {
        // All-day events: expand to each day in range
        // For all-day events, end date is exclusive (e.g., Dec 2-4 means Dec 2 and Dec 3)
        const current = new Date(start);
        while (current < end) {
          const dateKey = formatDatePerth(current);
          addEventToDate(dateKey, {
            summary,
            startMins: workStartMins,
            endMins: workEndMins,
            isAllDay: true
          });
          current.setDate(current.getDate() + 1);
        }
      } else {
        // Timed events: check if spans multiple days
        const startDateKey = formatDatePerth(start);
        const endDateKey = formatDatePerth(end);
        
        if (startDateKey === endDateKey) {
          // Single-day event
          const startHour = parseInt(start.toLocaleTimeString('en-AU', { hour: '2-digit', hour12: false, timeZone: 'Australia/Perth' }));
          const startMinute = parseInt(start.toLocaleTimeString('en-AU', { minute: '2-digit', timeZone: 'Australia/Perth' }));
          const endHour = parseInt(end.toLocaleTimeString('en-AU', { hour: '2-digit', hour12: false, timeZone: 'Australia/Perth' }));
          const endMinute = parseInt(end.toLocaleTimeString('en-AU', { minute: '2-digit', timeZone: 'Australia/Perth' }));
          
          addEventToDate(startDateKey, {
            summary,
            startMins: startHour * 60 + startMinute,
            endMins: endHour * 60 + endMinute,
            isAllDay: false
          });
        } else {
          // Multi-day timed event: first day goes to midnight, subsequent days midnight to end
          const startHour = parseInt(start.toLocaleTimeString('en-AU', { hour: '2-digit', hour12: false, timeZone: 'Australia/Perth' }));
          const startMinute = parseInt(start.toLocaleTimeString('en-AU', { minute: '2-digit', timeZone: 'Australia/Perth' }));
          
          // First day: start time to end of day (use work end or midnight)
          addEventToDate(startDateKey, {
            summary,
            startMins: startHour * 60 + startMinute,
            endMins: 24 * 60, // until end of day
            isAllDay: false
          });
          
          // Middle days (if any): full workday blocked
          const current = new Date(start);
          current.setDate(current.getDate() + 1);
          while (formatDatePerth(current) !== endDateKey && current < end) {
            addEventToDate(formatDatePerth(current), {
              summary,
              startMins: 0, // from start of day
              endMins: 24 * 60, // to end of day
              isAllDay: false
            });
            current.setDate(current.getDate() + 1);
          }
          
          // Last day: midnight to end time
          const endHour = parseInt(end.toLocaleTimeString('en-AU', { hour: '2-digit', hour12: false, timeZone: 'Australia/Perth' }));
          const endMinute = parseInt(end.toLocaleTimeString('en-AU', { minute: '2-digit', timeZone: 'Australia/Perth' }));
          addEventToDate(endDateKey, {
            summary,
            startMins: 0,
            endMins: endHour * 60 + endMinute,
            isAllDay: false
          });
        }
      }
    }
    
    // Calculate availability for each day (in Perth timezone)
    for (let i = 0; i < daysAhead; i++) {
      const date = new Date(now);
      date.setDate(now.getDate() + i);
      
      const dateKey = formatDatePerth(date);
      const dayName = getDayNamePerth(date);
      const dayOfWeek = getDayOfWeekPerth(date);
      
      // Skip weekends
      if (dayOfWeek === 0 || dayOfWeek === 6) continue;
      
      const dayEvents = eventsByDate[dateKey] || [];
      const eventDetails: DailyAvailability['events'] = [];
      
      // Collect all busy intervals (clamped to working hours)
      const busyIntervals: Array<{ start: number; end: number; summary: string; isAllDay: boolean }> = [];
      
      for (const evt of dayEvents) {
        // Clamp to working hours
        const effectiveStart = Math.max(evt.startMins, workStartMins);
        const effectiveEnd = Math.min(evt.endMins, workEndMins);
        
        if (effectiveStart < effectiveEnd) {
          busyIntervals.push({
            start: effectiveStart,
            end: effectiveEnd,
            summary: evt.summary,
            isAllDay: evt.isAllDay
          });
        }
      }
      
      // Merge overlapping intervals to avoid double-counting
      busyIntervals.sort((a, b) => a.start - b.start);
      const mergedIntervals: Array<{ start: number; end: number; summaries: string[]; isAllDay: boolean }> = [];
      
      for (const interval of busyIntervals) {
        if (mergedIntervals.length === 0) {
          mergedIntervals.push({
            start: interval.start,
            end: interval.end,
            summaries: [interval.summary],
            isAllDay: interval.isAllDay
          });
        } else {
          const last = mergedIntervals[mergedIntervals.length - 1];
          if (interval.start <= last.end) {
            // Overlapping - extend the end and add summary
            last.end = Math.max(last.end, interval.end);
            if (!last.summaries.includes(interval.summary)) {
              last.summaries.push(interval.summary);
            }
            last.isAllDay = last.isAllDay || interval.isAllDay;
          } else {
            // Non-overlapping - add new interval
            mergedIntervals.push({
              start: interval.start,
              end: interval.end,
              summaries: [interval.summary],
              isAllDay: interval.isAllDay
            });
          }
        }
      }
      
      // Calculate total busy minutes from merged intervals
      let busyMinutes = 0;
      for (const interval of mergedIntervals) {
        const duration = interval.end - interval.start;
        busyMinutes += duration;
        
        eventDetails.push({
          summary: interval.summaries.join(', ') + (interval.isAllDay ? ' (All Day)' : ''),
          startTime: formatMins(interval.start),
          endTime: formatMins(interval.end),
          duration
        });
      }
      
      // Cap busy minutes at work day length (shouldn't exceed, but safety check)
      busyMinutes = Math.min(busyMinutes, WORK_MINUTES_PER_DAY);
      const availableMinutes = WORK_MINUTES_PER_DAY - busyMinutes;
      const capacityPercent = Math.round((availableMinutes / WORK_MINUTES_PER_DAY) * 100);
      
      availability.push({
        date: dateKey,
        dayName,
        busyMinutes,
        availableMinutes,
        totalWorkMinutes: WORK_MINUTES_PER_DAY,
        events: eventDetails,
        capacityPercent
      });
    }
    
    console.log(`[Calendar Availability] Calculated availability for ${availability.length} working days`);
    return availability;
    
  } catch (error: any) {
    if (error.message && (error.message.includes('token expired') || error.message.includes('not authorized'))) {
      console.log('[Calendar Availability] Calendar not connected - returning empty availability');
    } else {
      console.error('[Calendar Availability] Error fetching calendar:', error);
    }
    return availability;
  }
}

// Check for jobs whose calendar events were deleted
async function syncDeletedEvents(workspaceId: number) {
  try {
    const { DatabaseStorage } = await import('./storage');
    const storage = new DatabaseStorage();
    const auth = await getAuthClient(workspaceId);
    const calendar = google.calendar({ version: 'v3', auth });
    
    // Get all jobs that have calendar event IDs
    const jobsWithEvents = await storage.getJobsWithCalendarEvents(workspaceId);
    
    for (const job of jobsWithEvents) {
      if (!job.calendarEventId) continue;
      
      try {
        // Try to fetch the calendar event
        await calendar.events.get({
          calendarId: 'primary',
          eventId: job.calendarEventId,
        });
        
        // Event exists, no action needed
      } catch (error: any) {
        if (error.status === 404) {
          // Calendar event was deleted, update job status
          console.log(`Job ${job.jobId}: calendar event was deleted, updating status to To Do`);
          
          await storage.updateJobStatus(job.id, 'To Do', workspaceId);
          await storage.updateJobBookingDate(job.id, null, workspaceId);
          await storage.updateJobCalendarEventId(job.id, null, workspaceId);
        }
      }
    }
    
  } catch (error) {
    console.error('Error checking for deleted events:', error);
  }
}