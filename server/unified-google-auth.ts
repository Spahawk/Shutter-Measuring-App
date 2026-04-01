import { google } from 'googleapis';
import { storage } from './storage';
import crypto from 'crypto';

const CALENDAR_SCOPES = [
  'https://www.googleapis.com/auth/calendar.events',
  'https://www.googleapis.com/auth/calendar.readonly'
];

const GMAIL_SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.modify'
];

const ALL_SCOPES = [...CALENDAR_SCOPES, ...GMAIL_SCOPES];

function generateHmac(data: string): string {
  const secret = process.env.SESSION_SECRET || process.env.REPL_ID || 'unified-auth-secret';
  return crypto.createHmac('sha256', secret).update(data).digest('hex');
}

function getOAuth2Client() {
  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  
  const redirectUri = 'https://spark-production-4be1.up.railway.app/api/google/callback';
  
  return { oauth2Client: new google.auth.OAuth2(clientId, clientSecret, redirectUri), redirectUri };
}

export async function getUnifiedAuthUrl(workspaceId: number, userId: string): Promise<string> {
  const { oauth2Client, redirectUri } = getOAuth2Client();
  
  console.log('[Unified Google Auth] Using redirect URI:', redirectUri);
  
  const nonce = crypto.randomUUID();
  const timestamp = Date.now();
  const dataToSign = `${nonce}:${workspaceId}:${userId}:${timestamp}`;
  const hmac = generateHmac(dataToSign);
  
  const expiresAt = new Date(timestamp + 10 * 60 * 1000);
  
  await storage.createPendingAuthRequest(nonce, workspaceId, userId, hmac, expiresAt);
  
  console.log(`[Unified Google Auth] Auth nonce created for user ${userId}, workspace ${workspaceId} (stored in database)`);
  
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: ALL_SCOPES,
    prompt: 'select_account consent',
    include_granted_scopes: true,
    state: nonce,
  });
  
  return authUrl;
}

export async function validateAuthNonce(nonce: string): Promise<{ workspaceId: number; userId: string } | null> {
  const pending = await storage.getPendingAuthRequest(nonce);
  
  if (!pending) {
    console.warn('[Unified Google Auth] Nonce not found in database - possible replay or expired');
    return null;
  }
  
  if (pending.used) {
    console.warn('[Unified Google Auth] Nonce already used - possible replay attack');
    await storage.deletePendingAuthRequest(nonce);
    return null;
  }
  
  if (new Date() > pending.expiresAt) {
    console.warn('[Unified Google Auth] Nonce expired');
    await storage.deletePendingAuthRequest(nonce);
    return null;
  }
  
  const timestamp = pending.expiresAt.getTime() - 10 * 60 * 1000;
  const dataToSign = `${nonce}:${pending.workspaceId}:${pending.userId}:${timestamp}`;
  const expectedHmac = generateHmac(dataToSign);
  
  if (pending.hmac !== expectedHmac) {
    console.error('[Unified Google Auth] HMAC mismatch - data integrity violation');
    await storage.deletePendingAuthRequest(nonce);
    return null;
  }
  
  await storage.markPendingAuthRequestUsed(nonce);
  await storage.deletePendingAuthRequest(nonce);
  
  console.log(`[Unified Google Auth] Auth nonce validated for user ${pending.userId}, workspace ${pending.workspaceId}`);
  return { workspaceId: pending.workspaceId, userId: pending.userId };
}

export async function handleUnifiedCallback(code: string, workspaceId: number): Promise<{ success: boolean; error?: string }> {
  const { oauth2Client } = getOAuth2Client();
  
  try {
    const { tokens } = await oauth2Client.getToken(code);
    
    if (!tokens.access_token) {
      throw new Error('No access token received from Google');
    }
    
    if (!tokens.refresh_token) {
      console.warn('[Unified Google Auth] No refresh token received - user may need to revoke access and re-authorize');
      throw new Error('No refresh token received. Please go to your Google Account settings, revoke access for this app, and try again.');
    }
    
    const expiryDate = tokens.expiry_date ? new Date(tokens.expiry_date) : null;
    
    await storage.saveGoogleCalendarToken({
      workspaceId,
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token,
      expiryDate,
    });
    
    await storage.saveGmailToken({
      workspaceId,
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token,
      expiryDate,
    });
    
    console.log('[Unified Google Auth] Both Calendar and Gmail credentials saved for workspace:', workspaceId);
    
    const { startCalendarSyncService } = await import('./google-calendar');
    startCalendarSyncService();
    
    return { success: true };
    
  } catch (error: any) {
    console.error('[Unified Google Auth] Error handling callback:', error);
    return { 
      success: false, 
      error: error.message || 'Unknown error during authorization'
    };
  }
}

export async function checkGoogleConnectionStatus(workspaceId: number): Promise<{
  calendarConnected: boolean;
  gmailConnected: boolean;
  fullyConnected: boolean;
}> {
  try {
    const calendarToken = await storage.getGoogleCalendarToken(workspaceId);
    const gmailToken = await storage.getGmailToken(workspaceId);
    
    let calendarConnected = false;
    let gmailConnected = false;
    
    if (calendarToken && calendarToken.accessToken && calendarToken.refreshToken) {
      try {
        const clientId = process.env.GOOGLE_CLIENT_ID!;
        const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
        
        const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
        oauth2Client.setCredentials({
          access_token: calendarToken.accessToken,
          refresh_token: calendarToken.refreshToken,
        });
        
        const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
        await calendar.calendarList.list({ maxResults: 1 });
        calendarConnected = true;
      } catch (err: any) {
        console.log('[Unified Google Auth] Calendar token validation failed:', err.message);
        calendarConnected = false;
      }
    }
    
    if (gmailToken && gmailToken.accessToken && gmailToken.refreshToken) {
      try {
        const clientId = process.env.GOOGLE_CLIENT_ID!;
        const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
        
        const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
        oauth2Client.setCredentials({
          access_token: gmailToken.accessToken,
          refresh_token: gmailToken.refreshToken,
        });
        
        const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
        await gmail.users.getProfile({ userId: 'me' });
        gmailConnected = true;
      } catch (err: any) {
        console.log('[Unified Google Auth] Gmail token validation failed:', err.message);
        gmailConnected = false;
      }
    }
    
    return {
      calendarConnected,
      gmailConnected,
      fullyConnected: calendarConnected && gmailConnected
    };
  } catch (error) {
    console.error('[Unified Google Auth] Error checking connection status:', error);
    return {
      calendarConnected: false,
      gmailConnected: false,
      fullyConnected: false
    };
  }
}

export async function disconnectGoogle(workspaceId: number): Promise<boolean> {
  try {
    await storage.deleteGoogleCalendarToken(workspaceId);
    await storage.deleteGmailToken(workspaceId);
    console.log('[Unified Google Auth] Disconnected both Calendar and Gmail for workspace:', workspaceId);
    return true;
  } catch (error) {
    console.error('[Unified Google Auth] Error disconnecting:', error);
    return false;
  }
}
