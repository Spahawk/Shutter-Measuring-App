import { google, gmail_v1 } from 'googleapis';
import { storage } from './storage';
import crypto from 'crypto';
import { extractJobSheetData } from './routes';

const GMAIL_SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.modify'
];

interface GmailCredentials {
  workspaceId: number;
  accessToken: string;
  refreshToken: string;
  expiryDate: Date | null;
}

interface EmailAttachment {
  filename: string;
  mimeType: string;
  data: string;
  size: number;
}

interface ProcessedEmail {
  messageId: string;
  subject: string;
  from: string;
  date: Date;
  attachments: EmailAttachment[];
  emailType: 'CM' | 'JobSheet' | 'JobBank' | 'Unknown';
  bodyHtml?: string; // For Job Bank emails where data is in the body
}

export async function loadGmailCredentials(workspaceId: number) {
  try {
    const tokenData = await storage.getGmailToken(workspaceId);
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
    console.error('Error loading Gmail credentials from database:', err);
    return null;
  }
}

export async function saveGmailCredentials(credentials: GmailCredentials) {
  try {
    await storage.saveGmailToken({
      workspaceId: credentials.workspaceId,
      accessToken: credentials.accessToken,
      refreshToken: credentials.refreshToken,
      expiryDate: credentials.expiryDate,
    });
    
    console.log('Gmail credentials saved. Workspace:', credentials.workspaceId);
  } catch (err) {
    console.error('Error saving Gmail credentials:', err);
    throw err;
  }
}

const pendingAuthRequests = new Map<string, { 
  workspaceId: number; 
  userId: string; 
  expiresAt: number;
  hmac: string;
  used: boolean;
}>();

function generateHmac(data: string): string {
  const secret = process.env.SESSION_SECRET || process.env.REPL_ID || 'gmail-auth-secret';
  return crypto.createHmac('sha256', secret).update(data).digest('hex');
}

export function getGmailAuthUrl(workspaceId: number, userId: string, origin?: string): string {
  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  
  // Use the origin from the request for correct redirect URI
  let redirectUri: string;
  if (origin) {
    redirectUri = `${origin}/api/gmail/callback`;
    console.log('[Gmail] Using redirect URI from request origin:', redirectUri);
  } else {
    const deployedDomain = 'spark-production-4be1.up.railway.app';
    const devDomain = process.env.REPLIT_DOMAINS?.split(',')[0];
    
    const possibleDomains = [
      deployedDomain,
      devDomain,
      'localhost:5000'
    ].filter(Boolean);
    
    redirectUri = `https://${possibleDomains[0]}/api/gmail/callback`;
    console.log('[Gmail] Using redirect URI from environment:', redirectUri);
  }
  
  console.log('');
  console.log('*** IMPORTANT: Add this redirect URI to your Google Cloud Console ***');
  console.log(`- ${redirectUri}`);
  console.log('');
  
  const nonce = crypto.randomUUID();
  const timestamp = Date.now();
  const dataToSign = `${nonce}:${workspaceId}:${userId}:${timestamp}`;
  const hmac = generateHmac(dataToSign);
  
  pendingAuthRequests.set(nonce, {
    workspaceId,
    userId,
    expiresAt: timestamp + 10 * 60 * 1000,
    hmac,
    used: false,
  });
  
  setTimeout(() => pendingAuthRequests.delete(nonce), 10 * 60 * 1000);
  
  console.log(`[Gmail] Auth nonce created for user ${userId}, workspace ${workspaceId}`);
  
  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: GMAIL_SCOPES,
    prompt: 'select_account consent',
    include_granted_scopes: true,
    state: nonce,
  });
  
  return authUrl;
}

export function validateAuthNonce(nonce: string): { workspaceId: number; userId: string } | null {
  const pending = pendingAuthRequests.get(nonce);
  
  if (!pending) {
    console.warn('[Gmail] Nonce not found - possible replay or expired');
    return null;
  }
  
  if (pending.used) {
    console.warn('[Gmail] Nonce already used - possible replay attack');
    pendingAuthRequests.delete(nonce);
    return null;
  }
  
  if (Date.now() > pending.expiresAt) {
    console.warn('[Gmail] Nonce expired');
    pendingAuthRequests.delete(nonce);
    return null;
  }
  
  const timestamp = pending.expiresAt - 10 * 60 * 1000;
  const dataToSign = `${nonce}:${pending.workspaceId}:${pending.userId}:${timestamp}`;
  const expectedHmac = generateHmac(dataToSign);
  
  if (pending.hmac !== expectedHmac) {
    console.error('[Gmail] HMAC mismatch - data integrity violation');
    pendingAuthRequests.delete(nonce);
    return null;
  }
  
  pending.used = true;
  pendingAuthRequests.delete(nonce);
  
  console.log(`[Gmail] Auth nonce validated for user ${pending.userId}, workspace ${pending.workspaceId}`);
  return { workspaceId: pending.workspaceId, userId: pending.userId };
}

export async function handleGmailCallback(code: string, workspaceId: number, origin?: string): Promise<boolean> {
  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  
  // Use the origin from the request for correct redirect URI
  let redirectUri: string;
  if (origin) {
    redirectUri = `${origin}/api/gmail/callback`;
  } else {
    const deployedDomain = 'spark-production-4be1.up.railway.app';
    const devDomain = process.env.REPLIT_DOMAINS?.split(',')[0];
    
    const possibleDomains = [
      deployedDomain,
      devDomain,
      'localhost:5000'
    ].filter(Boolean);
    
    redirectUri = `https://${possibleDomains[0]}/api/gmail/callback`;
  }
  
  console.log('[Gmail] Processing callback with redirect URI:', redirectUri);
  
  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  
  try {
    const { tokens } = await oauth2Client.getToken(code);
    
    if (!tokens.access_token) {
      throw new Error('No access token received from Google');
    }
    
    if (!tokens.refresh_token) {
      console.warn('[Gmail] No refresh token received - user may need to revoke access and re-authorize');
      throw new Error('No refresh token received. Please go to your Google Account settings, revoke access for this app, and try again.');
    }
    
    await saveGmailCredentials({
      workspaceId,
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token,
      expiryDate: tokens.expiry_date ? new Date(tokens.expiry_date) : null,
    });
    
    return true;
  } catch (error) {
    console.error('Error handling Gmail callback:', error);
    throw error;
  }
}

export async function getGmailClient(workspaceId: number): Promise<gmail_v1.Gmail | null> {
  const client = await loadGmailCredentials(workspaceId);
  if (!client) {
    return null;
  }
  
  return google.gmail({ version: 'v1', auth: client });
}

export async function isGmailConnected(workspaceId: number): Promise<boolean> {
  try {
    const gmail = await getGmailClient(workspaceId);
    if (!gmail) return false;
    
    await gmail.users.getProfile({ userId: 'me' });
    return true;
  } catch (error) {
    console.error('Gmail connection check failed:', error);
    return false;
  }
}

export async function disconnectGmail(workspaceId: number): Promise<void> {
  await storage.deleteGmailToken(workspaceId);
}

export async function searchEmails(gmail: gmail_v1.Gmail, query: string, maxResults: number = 10) {
  const response = await gmail.users.messages.list({
    userId: 'me',
    q: query,
    maxResults,
  });
  return response.data.messages || [];
}

export async function getEmailContent(gmail: gmail_v1.Gmail, messageId: string) {
  const response = await gmail.users.messages.get({
    userId: 'me',
    id: messageId,
    format: 'full',
  });
  return response.data;
}

export function decodeBase64(data: string): string {
  return Buffer.from(data.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf-8');
}

function classifyEmail(subject: string): 'CM' | 'JobSheet' | 'JobBank' | 'Unknown' {
  const lowerSubject = subject.toLowerCase().trim();
  
  // Check for Job Bank first (Wednesday install cost emails)
  if (lowerSubject.includes('job bank')) {
    return 'JobBank';
  }
  
  // Check for CM - abbreviated form
  if (lowerSubject === 'cm' || lowerSubject.startsWith('cm ') || lowerSubject.startsWith('cm-') || lowerSubject.startsWith('cm -') || lowerSubject.includes(' cm ') || lowerSubject.endsWith(' cm')) {
    return 'CM';
  }
  
  // Check for "Check Measure" - full phrase (e.g., "Check Measure - Client Name")
  if (lowerSubject.includes('check measure') || lowerSubject.includes('checkmeasure')) {
    return 'CM';
  }
  
  if (lowerSubject.includes('job sheet') || lowerSubject.includes('jobsheet')) {
    return 'JobSheet';
  }
  
  return 'Unknown';
}

async function getAttachment(gmail: gmail_v1.Gmail, messageId: string, attachmentId: string): Promise<string> {
  const attachment = await gmail.users.messages.attachments.get({
    userId: 'me',
    messageId,
    id: attachmentId,
  });
  
  return attachment.data.data || '';
}

async function getEmailDetails(gmail: gmail_v1.Gmail, messageId: string): Promise<ProcessedEmail | null> {
  try {
    const message = await gmail.users.messages.get({
      userId: 'me',
      id: messageId,
      format: 'full',
    });
    
    const headers = message.data.payload?.headers || [];
    const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value || '';
    const from = headers.find(h => h.name?.toLowerCase() === 'from')?.value || '';
    const dateStr = headers.find(h => h.name?.toLowerCase() === 'date')?.value || '';
    
    const emailType = classifyEmail(subject);
    const attachments: EmailAttachment[] = [];
    let bodyHtml = '';
    
    const parts = message.data.payload?.parts || [];
    for (const part of parts) {
      // Extract HTML body for Job Bank emails
      if (part.mimeType === 'text/html' && part.body?.data) {
        bodyHtml = Buffer.from(part.body.data, 'base64').toString('utf-8');
      }
      
      if (part.filename && part.body?.attachmentId) {
        const isHtml = part.mimeType === 'text/html' || part.filename.endsWith('.html') || part.filename.endsWith('.htm');
        
        if (isHtml) {
          const data = await getAttachment(gmail, messageId, part.body.attachmentId);
          attachments.push({
            filename: part.filename,
            mimeType: part.mimeType || 'text/html',
            data,
            size: part.body.size || 0,
          });
        }
      }
    }
    
    // Check direct body if no parts
    if (message.data.payload?.body?.data) {
      const bodyData = Buffer.from(message.data.payload.body.data, 'base64').toString('utf-8');
      if (bodyData.includes('<html') || bodyData.includes('<table')) {
        if (!bodyHtml) {
          bodyHtml = bodyData;
        }
        if (!attachments.length && emailType !== 'JobBank') {
          attachments.push({
            filename: 'email-body.html',
            mimeType: 'text/html',
            data: Buffer.from(bodyData).toString('base64'),
            size: bodyData.length,
          });
        }
      }
    }
    
    return {
      messageId,
      subject,
      from,
      date: new Date(dateStr),
      attachments,
      emailType,
      bodyHtml: emailType === 'JobBank' ? bodyHtml : undefined,
    };
  } catch (error) {
    console.error('Error getting email details:', error);
    return null;
  }
}

export async function fetchNewEmails(workspaceId: number): Promise<ProcessedEmail[]> {
  const gmail = await getGmailClient(workspaceId);
  if (!gmail) {
    throw new Error('Gmail not connected');
  }
  
  const processedIds = await storage.getProcessedEmailIds(workspaceId);
  const processedSet = new Set(processedIds);
  
  // Search for CM, Check Measure, Job Sheet (with attachments) AND Job Bank emails
  const queries = [
    'subject:(CM OR "Check Measure" OR "Job Sheet") has:attachment',
    'subject:"Job Bank"'
  ];
  
  const newEmails: ProcessedEmail[] = [];
  
  try {
    for (const query of queries) {
      const response = await gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 50,
      });
      
      const messages = response.data.messages || [];
      
      for (const msg of messages) {
        if (msg.id && !processedSet.has(msg.id)) {
          const emailDetails = await getEmailDetails(gmail, msg.id);
          if (emailDetails && emailDetails.emailType !== 'Unknown') {
            // For CM/JobSheet, require attachments. For JobBank, require body HTML
            if (emailDetails.emailType === 'JobBank') {
              if (emailDetails.bodyHtml) {
                newEmails.push(emailDetails);
              }
            } else if (emailDetails.attachments.length > 0) {
              newEmails.push(emailDetails);
            }
          }
        }
      }
    }
    
    return newEmails;
  } catch (error) {
    console.error('Error fetching emails:', error);
    throw error;
  }
}

export async function markEmailAsProcessed(workspaceId: number, messageId: string, subject?: string, emailType?: string): Promise<void> {
  await storage.markEmailProcessed(workspaceId, messageId, subject, emailType);
}

export interface JobBankRow {
  jobId: string;
  clientName: string;
  address: string;
  type: string;
  eta: string;
  installCost: string;
}

// Helper to clean cell text
function getCellText(cell: string): string {
  return cell.replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').trim();
}

// Helper to parse currency values like "$322.76" or "$1,234.50"
function parseCurrency(value: string): string {
  if (!value) return '';
  // Remove $ and commas, then parse
  const cleaned = value.replace(/[$,]/g, '').trim();
  const num = parseFloat(cleaned);
  if (isNaN(num)) return '';
  return num.toFixed(2);
}

export function parseJobBankTable(htmlContent: string): JobBankRow[] {
  const rows: JobBankRow[] = [];
  
  // Find all tables in the HTML
  const tableMatches = htmlContent.match(/<table[^>]*>[\s\S]*?<\/table>/gi) || [];
  
  // Find the correct data table by looking for header row with expected columns
  let dataTableHtml = '';
  for (const tableHtml of tableMatches) {
    const headerRow = tableHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/i);
    if (headerRow) {
      const headerText = getCellText(headerRow[0]).toLowerCase();
      // Look for the table with job-related headers
      if ((headerText.includes('job') || headerText.includes('customer')) && 
          (headerText.includes('address') || headerText.includes('site')) &&
          (headerText.includes('install') || headerText.includes('cost'))) {
        dataTableHtml = tableHtml;
        break;
      }
    }
  }
  
  // If no table found with exact headers, try to find any table with job data
  if (!dataTableHtml) {
    for (const tableHtml of tableMatches) {
      // Look for tables that contain what looks like job IDs (e.g., J0006789)
      if (/J\d{6,}/i.test(tableHtml)) {
        dataTableHtml = tableHtml;
        break;
      }
    }
  }
  
  if (!dataTableHtml) {
    console.log('[JobBank] No suitable data table found');
    return rows;
  }
  
  // Extract all rows from the data table
  const rowMatches = dataTableHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) || [];
  console.log(`[JobBank] Found ${rowMatches.length} rows in data table`);
  
  // Determine column indices from header row
  let jobIdCol = 0, clientCol = 1, addressCol = 2, typeCol = 3, etaCol = 4, costCol = 5;
  
  if (rowMatches.length > 0 && rowMatches[0]) {
    const headerCells = rowMatches[0].match(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/gi) || [];
    for (let i = 0; i < headerCells.length; i++) {
      const headerText = getCellText(headerCells[i]).toLowerCase();
      if (headerText.includes('job') && !headerText.includes('type')) jobIdCol = i;
      else if (headerText.includes('customer') || headerText.includes('client')) clientCol = i;
      else if (headerText.includes('address') || headerText.includes('site')) addressCol = i;
      else if (headerText.includes('type')) typeCol = i;
      else if (headerText.includes('eta') || headerText.includes('date')) etaCol = i;
      else if (headerText.includes('install') || headerText.includes('cost')) costCol = i;
    }
  }
  
  // Skip header row, process data rows
  for (let i = 1; i < rowMatches.length; i++) {
    const row = rowMatches[i];
    const cells = row.match(/<td[^>]*>([\s\S]*?)<\/td>/gi) || [];
    
    if (cells.length >= 5) {
      const jobId = getCellText(cells[jobIdCol] || '');
      const clientName = getCellText(cells[clientCol] || '');
      const address = getCellText(cells[addressCol] || '');
      const type = getCellText(cells[typeCol] || '');
      const eta = getCellText(cells[etaCol] || '');
      const installCostRaw = getCellText(cells[costCol] || '');
      const installCost = parseCurrency(installCostRaw);
      
      // Only add if we have a valid job ID (format: J followed by digits, or just digits)
      if (jobId && /^J?\d+/i.test(jobId)) {
        rows.push({
          jobId: jobId.toUpperCase(),
          clientName,
          address,
          type,
          eta,
          installCost
        });
        console.log(`[JobBank] Parsed row: ${jobId}, cost: ${installCostRaw} -> ${installCost}`);
      }
    }
  }
  
  console.log(`[JobBank] Successfully parsed ${rows.length} job rows`);
  return rows;
}

export async function processEmailAttachments(
  workspaceId: number,
  emails: ProcessedEmail[],
  processJobSheet: (htmlContent: string) => Promise<any>,
  processCMSheet: (htmlContent: string) => Promise<any>,
  processJobBank?: (rows: JobBankRow[]) => Promise<{ created: number; updated: number }>
): Promise<{
  jobsCreated: number;
  cmsCreated: number;
  jobBankProcessed: number;
  errors: string[];
}> {
  const results = {
    jobsCreated: 0,
    cmsCreated: 0,
    jobBankProcessed: 0,
    errors: [] as string[],
  };
  
  for (const email of emails) {
    try {
      // Handle Job Bank emails (table data in body)
      if (email.emailType === 'JobBank' && email.bodyHtml && processJobBank) {
        try {
          const rows = parseJobBankTable(email.bodyHtml);
          console.log(`[Gmail] Job Bank email found with ${rows.length} jobs`);
          const result = await processJobBank(rows);
          results.jobBankProcessed += rows.length;
          results.jobsCreated += result.created;
          console.log(`[Gmail] Job Bank: ${result.created} created, ${result.updated} updated`);
        } catch (error: any) {
          results.errors.push(`Job Bank from ${email.subject}: ${error.message}`);
        }
        await markEmailAsProcessed(workspaceId, email.messageId, email.subject, email.emailType);
        continue;
      }
      
      // Handle CM and JobSheet emails (attachments)
      for (const attachment of email.attachments) {
        const htmlContent = Buffer.from(attachment.data, 'base64').toString('utf-8');
        
        if (email.emailType === 'JobSheet') {
          try {
            await processJobSheet(htmlContent);
            results.jobsCreated++;
          } catch (error: any) {
            results.errors.push(`Job sheet from ${email.subject}: ${error.message}`);
          }
        } else if (email.emailType === 'CM') {
          try {
            await processCMSheet(htmlContent);
            results.cmsCreated++;
          } catch (error: any) {
            results.errors.push(`CM from ${email.subject}: ${error.message}`);
          }
        }
      }
      
      await markEmailAsProcessed(workspaceId, email.messageId, email.subject, email.emailType);
    } catch (error: any) {
      results.errors.push(`Email ${email.subject}: ${error.message}`);
    }
  }
  
  return results;
}

// Auto-sync service for Gmail
let gmailSyncInterval: NodeJS.Timeout | null = null;

export function startGmailSyncService() {
  if (gmailSyncInterval) {
    clearInterval(gmailSyncInterval);
  }
  
  // Start syncing every 15 minutes
  gmailSyncInterval = setInterval(async () => {
    try {
      await autoSyncGmail();
    } catch (error) {
      console.error('[Gmail Auto-Sync] Error:', error);
    }
  }, 15 * 60 * 1000); // 15 minutes
  
  console.log('[Gmail Auto-Sync] Service started - checking for new emails every 15 minutes');
  
  // Run initial sync after 30 seconds
  setTimeout(async () => {
    try {
      console.log('[Gmail Auto-Sync] Running initial sync...');
      await autoSyncGmail();
    } catch (error) {
      console.error('[Gmail Auto-Sync] Initial sync error:', error);
    }
  }, 30 * 1000);
}

export function stopGmailSyncService() {
  if (gmailSyncInterval) {
    clearInterval(gmailSyncInterval);
    gmailSyncInterval = null;
    console.log('[Gmail Auto-Sync] Service stopped');
  }
}

async function autoSyncGmail() {
  // Get all workspaces with Gmail connected
  const gmailTokens = await storage.getAllGmailTokens();
  
  if (!gmailTokens || gmailTokens.length === 0) {
    console.log('[Gmail Auto-Sync] No Gmail connections found');
    return;
  }
  
  for (const token of gmailTokens) {
    try {
      console.log(`[Gmail Auto-Sync] Syncing workspace ${token.workspaceId}...`);
      
      const emails = await fetchNewEmails(token.workspaceId);
      
      if (emails.length === 0) {
        console.log(`[Gmail Auto-Sync] Workspace ${token.workspaceId}: No new emails`);
        continue;
      }
      
      console.log(`[Gmail Auto-Sync] Workspace ${token.workspaceId}: Found ${emails.length} new emails`);
      
      // Process Job Bank emails
      const jobBankEmails = emails.filter(e => e.emailType === 'JobBank');
      for (const email of jobBankEmails) {
        if (email.bodyHtml) {
          try {
            const rows = parseJobBankTable(email.bodyHtml);
            console.log(`[Gmail Auto-Sync] Processing Job Bank with ${rows.length} jobs`);
            
            // Update existing jobs with new ETA and install cost
            for (const row of rows) {
              await updateJobFromJobBank(token.workspaceId, row);
            }
            
            await markEmailAsProcessed(token.workspaceId, email.messageId, email.subject, email.emailType);
            console.log(`[Gmail Auto-Sync] Job Bank processed: ${rows.length} jobs updated`);
          } catch (error) {
            console.error(`[Gmail Auto-Sync] Job Bank error:`, error);
          }
        }
      }
      
      // Process CM and Job Sheet emails
      const attachmentEmails = emails.filter(e => e.emailType === 'CM' || e.emailType === 'JobSheet');
      for (const email of attachmentEmails) {
        for (const attachment of email.attachments) {
          try {
            const htmlContent = Buffer.from(attachment.data, 'base64').toString('utf-8');
            
            if (email.emailType === 'JobSheet') {
              await processJobSheetAuto(token.workspaceId, htmlContent);
              console.log(`[Gmail Auto-Sync] Processed Job Sheet: ${email.subject}`);
            } else if (email.emailType === 'CM') {
              await processCMSheetAuto(token.workspaceId, htmlContent, email.subject);
              console.log(`[Gmail Auto-Sync] Processed CM: ${email.subject}`);
            }
          } catch (error) {
            console.error(`[Gmail Auto-Sync] Attachment error:`, error);
          }
        }
        await markEmailAsProcessed(token.workspaceId, email.messageId, email.subject, email.emailType);
      }
      
    } catch (error) {
      console.error(`[Gmail Auto-Sync] Workspace ${token.workspaceId} error:`, error);
    }
  }
}

// Helper function to update job from Job Bank data
async function updateJobFromJobBank(workspaceId: number, row: JobBankRow) {
  try {
    // Find existing job by jobId
    const existingJobs = await storage.getJobsByJobIds([row.jobId], workspaceId);
    const existingJob = existingJobs[0];
    
    // Helper: check if value is a placeholder (not a real date)
    const placeholderValues = ['pending', 'pending eta', 'tba', 'tbc', 'n/a', 'unknown'];
    const isPlaceholder = (val: string) => placeholderValues.includes(val.toLowerCase().trim());
    const hasRealValue = (val: string | null | undefined) => {
      if (!val) return false;
      const trimmed = val.trim();
      return trimmed !== '' && !isPlaceholder(trimmed);
    };
    
    if (existingJob) {
      // Only update ETA if it's currently "Pending" or empty
      const updates: any = {};
      
      // Only update if incoming ETA is a real value (not "Pending") AND existing ETA is placeholder/empty
      if (hasRealValue(row.eta)) {
        const currentEta = existingJob.eta?.toLowerCase().trim() || '';
        // Update if ETA is pending, empty, or contains "pending"
        if (currentEta === 'pending' || currentEta === 'pending eta' || currentEta === '' || !existingJob.eta || currentEta.includes('pending')) {
          updates.eta = row.eta;
          console.log(`[Gmail Auto-Sync] Updating ETA for ${row.jobId}: "${existingJob.eta}" -> "${row.eta}"`);
        }
      }
      
      // Update install cost if provided and job doesn't have one
      if (row.installCost !== undefined && row.installCost !== null) {
        if (!existingJob.installCost || existingJob.installCost === '0' || existingJob.installCost === '') {
          updates.installCost = row.installCost.toString();
          console.log(`[Gmail Auto-Sync] Updating install cost for ${row.jobId}: $${row.installCost}`);
        }
      }
      
      if (Object.keys(updates).length > 0) {
        await storage.updateJob(existingJob.id, updates, workspaceId);
      }
    } else {
      console.log(`[Gmail Auto-Sync] Job ${row.jobId} not found - skipping`);
    }
  } catch (error) {
    console.error(`[Gmail Auto-Sync] Error updating job ${row.jobId}:`, error);
  }
}

// Helper function to process job sheet automatically
async function processJobSheetAuto(workspaceId: number, htmlContent: string) {
  // Extract job data from HTML
  const jobIdMatch = htmlContent.match(/Job\s*(?:ID|Number|No\.?)?\s*:?\s*([A-Z]?\d+[-/]?\d*)/i);
  if (!jobIdMatch) return;
  
  const jobId = jobIdMatch[1].toUpperCase();
  const existingJobs = await storage.getJobsByJobIds([jobId], workspaceId);
  const existingJob = existingJobs[0];
  
  if (existingJob) {
    // Store the job sheet document
    await storage.createJobDocument({
      jobId: existingJob.id,
      documentType: 'jobsheet',
      fileName: `${jobId}_jobsheet.html`,
      originalContent: htmlContent,
    });
  }
}

// Helper function to process CM sheet automatically
// Uses the same extraction logic as manual /api/gmail/sync for consistent CM job creation
async function processCMSheetAuto(workspaceId: number, htmlContent: string, emailSubject?: string): Promise<{ created: boolean; jobId?: string }> {
  try {
    // Use the same extraction function as manual sync
    const extractedData = extractJobSheetData(htmlContent);
    
    if (!extractedData.jobId) {
      console.log('[Gmail Auto-Sync] Could not extract job ID from CM document');
      return { created: false };
    }
    
    // If no valid client name extracted, try to extract from email subject
    // Subject format: "CM - Giudicatti J0015964-1 - CHURCHLANDS" or "Check Measure - ClientName - ..."
    let clientName = extractedData.clientName;
    const invalidClientNames = ['Unknown Client', 'Julie Harris', 'Michelle Fryer', 'Frances French', 
      'Cheryl Collister', 'Elena Deighan', 'Renata Victor', 'Tara Carle', 'Alida Miller', 'Lyn Sullivan',
      'Emerson Redondo'];
    
    if (!clientName || invalidClientNames.includes(clientName)) {
      if (emailSubject) {
        console.log(`[Gmail Auto-Sync] Trying to extract client name from subject: "${emailSubject}"`);
        // Pattern: "CM - ClientName JobId" or "Check Measure - ClientName"
        // Examples: "CM - Giudicatti J0015964-1 - CHURCHLANDS", "Check measure"
        const subjectPatterns = [
          /(?:CM|Check\s*Measure)\s*[-–]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+J\d+/i,  // "CM - Giudicatti J0015964"
          /(?:CM|Check\s*Measure)\s*[-–]\s*([A-Z][a-z]+)\s*[-–]/i,  // "CM - ClientLastName -"
        ];
        
        for (const pattern of subjectPatterns) {
          const match = emailSubject.match(pattern);
          if (match && match[1]) {
            clientName = match[1].trim();
            console.log(`[Gmail Auto-Sync] Extracted client name from subject: "${clientName}"`);
            break;
          }
        }
      }
    }
    
    // Create CM job ID by appending -CM (same as manual sync)
    const cmJobId = `${extractedData.jobId}-CM`;
    
    // Check if CM job already exists
    const existingJobsList = await storage.getJobsByJobIds([cmJobId], workspaceId);
    const existingJob = existingJobsList.find(j => j.jobId === cmJobId && j.isCheckMeasureJob);
    
    if (existingJob) {
      // Update empty fields in existing job (bi-directional merge)
      const isEmpty = (val: any) => val === null || val === undefined || (typeof val === 'string' && val.trim() === '');
      const hasValue = (val: any) => val !== null && val !== undefined && (typeof val !== 'string' || val.trim() !== '');
      
      const updateData: any = {};
      let fieldsUpdated = 0;
      
      if (isEmpty(existingJob.clientName) && hasValue(clientName)) {
        updateData.clientName = clientName;
        fieldsUpdated++;
      }
      if (isEmpty(existingJob.address) && hasValue(extractedData.address)) {
        updateData.address = extractedData.address;
        fieldsUpdated++;
      }
      if (isEmpty(existingJob.phoneNumber) && extractedData.contact && hasValue(extractedData.contact)) {
        const phoneMatch = extractedData.contact.match(/[\d\s\-+()]+/);
        if (phoneMatch) {
          updateData.phoneNumber = phoneMatch[0].trim();
          fieldsUpdated++;
        }
      }
      
      if (fieldsUpdated > 0) {
        await storage.updateJob(existingJob.id, updateData, workspaceId);
        console.log(`[Gmail Auto-Sync] Updated existing CM job ${cmJobId} with ${fieldsUpdated} fields`);
      }
      
      // Check if CM document already exists
      const existingDocs = await storage.getJobDocuments(existingJob.id);
      const hasCM = existingDocs.some(d => d.documentType === 'cm');
      
      if (!hasCM) {
        await storage.createJobDocument({
          jobId: existingJob.id,
          documentType: 'cm',
          fileName: `${cmJobId}_cm.html`,
          originalContent: htmlContent,
        });
        console.log(`[Gmail Auto-Sync] CM document added to existing job ${cmJobId}`);
      }
      
      return { created: false, jobId: cmJobId };
    } else {
      // Create new CM job (same logic as manual sync)
      const cmJobData = {
        workspaceId,
        jobId: cmJobId,
        clientName: clientName || 'Unknown Client',
        address: extractedData.address || 'Address not found',
        type: (extractedData.type || 'Install') as 'Install' | 'Service',
        eta: extractedData.eta || 'Pending',
        status: 'To Do' as const,
        urgent: false,
        isCheckMeasureJob: true,
        hasCheckMeasure: true,
        phoneNumber: extractedData.contact ? extractedData.contact.match(/[\d\s\-+()]+/)?.[0]?.trim() : undefined,
        consultant: extractedData.consultant || undefined,
      };
      
      const newJob = await storage.createJob(cmJobData);
      console.log(`[Gmail Auto-Sync] Created new CM job: ${cmJobId} - ${clientName}`);
      
      // Store the CM document
      await storage.createJobDocument({
        jobId: newJob.id,
        documentType: 'cm',
        fileName: `${cmJobId}_cm.html`,
        originalContent: htmlContent,
      });
      console.log(`[Gmail Auto-Sync] CM document stored for new job ${cmJobId}`);
      
      return { created: true, jobId: cmJobId };
    }
  } catch (error) {
    console.error(`[Gmail Auto-Sync] Error processing CM:`, error);
    return { created: false };
  }
}

export interface InvoiceEmailResult {
  messageId: string;
  subject: string;
  date: string;
  snippet: string;
  bodyText: string;
  bodyHtml: string;
  hasAttachments: boolean;
  attachmentNames: string[];
  pdfTexts: string[];
  senderEmail: string;
  senderName: string;
}

export interface ParsedInvoice {
  messageId: string;
  invoiceNumber: string | null;
  amount: number | null;
  date: string;
  weekEnding: string;
  company: string | null;
  senderEmail: string;
  senderName: string;
}

function parseInvoiceAmount(bodyText: string, bodyHtml: string, snippet: string, pdfTexts?: string[]): { amount: number | null; invoiceNumber: string | null } {
  let amount: number | null = null;
  let invoiceNumber: string | null = null;

  const stripHtmlTags = (s: string) => s.replace(/<[^>]*>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#\d+;/g, '').replace(/&[a-z]+;/g, '').replace(/[\u200B\u200C\u200D\uFEFF\u00AD\u2028\u2029\u202A-\u202E\u2060-\u2064\u2066-\u206F]/g, '').replace(/\s+/g, ' ');

  const stripInvisible = (s: string) => s.replace(/[\u200B\u200C\u200D\uFEFF\u00AD\u2028\u2029\u202A-\u202E\u2060-\u2064\u2066-\u206F]/g, '');
  const sources = [
    ...(pdfTexts || []).map(stripInvisible),
    stripInvisible(bodyText || ''),
    bodyHtml ? stripHtmlTags(bodyHtml) : '',
    stripInvisible(snippet || ''),
  ].filter(Boolean);

  const invPatterns = [
    /Invoice\s*#\s*:?\s*([A-Z0-9\-]+)/i,
    /Invoice\s*(?:Number|No\.?|Num)\s*:?\s*([A-Z0-9\-]+)/i,
    /INV[\-#]?\s*(\d+)/i,
    /Reference\s*:?\s*([A-Z0-9\-]+)/i,
  ];

  for (const text of sources) {
    if (!invoiceNumber) {
      for (const pattern of invPatterns) {
        const m = text.match(pattern);
        if (m) { invoiceNumber = m[1]; break; }
      }
    }
  }

  const totalPatterns = [
    /(?:Total|Amount\s*Due|Balance\s*Due|Grand\s*Total|Total\s*Due|Amount\s*Payable)\s*:?\s*\$\s*([\d,]+\.\d{2})/i,
    /(?:Total|Amount\s*Due|Balance\s*Due|Grand\s*Total|Total\s*Due|Amount\s*Payable)\s*:?\s*(?:AUD|USD|GBP|EUR|NZD|CAD)?\s*\$?\s*([\d,]+\.\d{2})/i,
    /(?:Total|Amount\s*Due|Balance\s*Due|Grand\s*Total)\s*:?\s*[\u00A3\u20AC\u00A5]\s*([\d,]+\.\d{2})/i,
  ];

  const fallbackAmountPatterns = [
    /\$\s*([\d,]+\.\d{2})/,
    /(?:AUD|USD|GBP|EUR|NZD|CAD)\s*\$?\s*([\d,]+\.\d{2})/i,
    /[\u00A3\u20AC]\s*([\d,]+\.\d{2})/,
  ];

  for (const text of sources) {
    if (amount !== null) break;
    for (const pattern of totalPatterns) {
      const m = text.match(pattern);
      if (m) {
        amount = parseFloat(m[1].replace(/,/g, ''));
        break;
      }
    }
  }

  if (amount === null) {
    for (const text of sources) {
      if (amount !== null) break;
      for (const pattern of fallbackAmountPatterns) {
        const matches = [...text.matchAll(new RegExp(pattern.source, pattern.flags + 'g'))];
        if (matches.length > 0) {
          const last = matches[matches.length - 1];
          amount = parseFloat(last[1].replace(/,/g, ''));
          break;
        }
      }
    }
  }

  return { amount, invoiceNumber };
}

function parseBillTo(bodyText: string, bodyHtml: string): string | null {
  const stripHtml = (s: string) => s.replace(/<[^>]*>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/&#\d+;/g, '').replace(/&[a-z]+;/g, '').replace(/\s+/g, ' ');

  const sources = [bodyText, bodyHtml ? stripHtml(bodyHtml) : ''].filter(Boolean);
  for (const text of sources) {
    const patterns = [
      /Bill\s+[Tt]o[\s:]+([A-Za-z0-9 &'\-\.]+?)(?:\s{2,}|\n|Address|Phone|Email|ABN|$)/m,
      /Billed\s+[Tt]o[\s:]+([A-Za-z0-9 &'\-\.]+?)(?:\s{2,}|\n|Address|Phone|Email|ABN|$)/m,
      /BILL\s+TO[\s:]+([A-Za-z0-9 &'\-\.]+?)(?:\s{2,}|\n|Address|Phone|Email|ABN|$)/m,
    ];
    for (const re of patterns) {
      const m = text.match(re);
      if (m) {
        const name = m[1].trim();
        if (name.length > 1 && name.length < 60) return name;
      }
    }
  }
  return null;
}

function getWeekEnding(dateStr: string): string {
  const d = new Date(dateStr);
  const day = d.getDay();
  const diff = day === 0 ? 0 : 7 - day;
  const sunday = new Date(d);
  sunday.setDate(d.getDate() + diff);
  return sunday.toISOString().split('T')[0];
}

export function parseInvoiceEmails(emails: InvoiceEmailResult[]): ParsedInvoice[] {
  return emails.map(e => {
    const { amount, invoiceNumber } = parseInvoiceAmount(e.bodyText, e.bodyHtml, e.snippet, e.pdfTexts);
    const company = parseBillTo(e.bodyText, e.bodyHtml);
    return {
      messageId: e.messageId,
      invoiceNumber,
      amount,
      date: e.date,
      weekEnding: getWeekEnding(e.date),
      company,
      senderEmail: e.senderEmail,
      senderName: e.senderName,
    };
  });
}

export async function loadSecondaryGmailCredentials(workspaceId: number) {
  try {
    const tokenData = await storage.getSecondaryGmailToken(workspaceId);
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

    oauth2Client.on('tokens', async (tokens) => {
      if (tokens.access_token) {
        await storage.saveSecondaryGmailToken({
          workspaceId,
          accessToken: tokens.access_token,
          refreshToken: tokenData.refreshToken,
          expiryDate: tokens.expiry_date ? new Date(tokens.expiry_date) : null,
          emailAddress: tokenData.emailAddress,
          purpose: 'invoices',
        });
      }
    });
    
    return oauth2Client;
  } catch (err) {
    console.error('Error loading secondary Gmail credentials:', err);
    return null;
  }
}

export async function getSecondaryGmailClient(workspaceId: number): Promise<gmail_v1.Gmail | null> {
  const auth = await loadSecondaryGmailCredentials(workspaceId);
  if (!auth) return null;
  return google.gmail({ version: 'v1', auth });
}

function getSecondaryRedirectUri(origin?: string): string {
  return 'https://spark-production-4be1.up.railway.app/api/google/callback';
}

function makeSecondaryState(workspaceId: number, userId: string): string {
  const timestamp = Date.now().toString();
  const payload = `secondary_gmail:${workspaceId}:${userId}:${timestamp}`;
  const sig = generateHmac(payload);
  return Buffer.from(JSON.stringify({ payload, sig })).toString('base64url');
}

export function isSecondaryGmailState(state: string): boolean {
  try {
    const parsed = JSON.parse(Buffer.from(state, 'base64url').toString('utf8'));
    return parsed.payload?.startsWith('secondary_gmail:') || false;
  } catch {
    return false;
  }
}

function parseSecondaryState(state: string): { workspaceId: number; userId: string } {
  let parsed: any;
  try {
    parsed = JSON.parse(Buffer.from(state, 'base64url').toString('utf8'));
  } catch {
    throw new Error('Invalid state parameter');
  }
  const { payload, sig } = parsed;
  if (!payload || !sig) throw new Error('Malformed state parameter');
  const expectedSig = generateHmac(payload);
  if (sig !== expectedSig) throw new Error('State signature invalid');
  const parts = payload.split(':');
  if (parts[0] === 'secondary_gmail') {
    if (parts.length < 4) throw new Error('Malformed state payload');
    const timestamp = parseInt(parts[3], 10);
    if (Date.now() - timestamp > 15 * 60 * 1000) throw new Error('Auth request expired');
    return { workspaceId: parseInt(parts[1], 10), userId: parts[2] };
  }
  if (parts.length < 3) throw new Error('Malformed state payload');
  const timestamp = parseInt(parts[2], 10);
  if (Date.now() - timestamp > 15 * 60 * 1000) throw new Error('Auth request expired');
  return { workspaceId: parseInt(parts[0], 10), userId: parts[1] };
}

export function getSecondaryGmailAuthUrl(workspaceId: number, userId: string, origin?: string): string {
  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  const redirectUri = getSecondaryRedirectUri(origin);
  console.log('[Gmail] Secondary auth URL using redirect URI:', redirectUri);
  const state = makeSecondaryState(workspaceId, userId);
  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  return oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: GMAIL_SCOPES,
    state,
    prompt: 'consent',
  });
}

export async function handleSecondaryGmailCallback(code: string, state: string, origin?: string) {
  const { workspaceId, userId } = parseSecondaryState(state);

  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  const redirectUri = getSecondaryRedirectUri(origin);

  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  const { tokens } = await oauth2Client.getToken(code);

  oauth2Client.setCredentials(tokens);
  const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
  const profile = await gmail.users.getProfile({ userId: 'me' });
  const emailAddress = profile.data.emailAddress || '';

  await storage.saveSecondaryGmailToken({
    workspaceId,
    emailAddress,
    accessToken: tokens.access_token!,
    refreshToken: tokens.refresh_token!,
    expiryDate: tokens.expiry_date ? new Date(tokens.expiry_date) : null,
    purpose: 'invoices',
  });

  return { emailAddress };
}

async function fetchInvoiceEmailsFromClient(gmail: gmail_v1.Gmail, label: string, customQueries?: string[]): Promise<InvoiceEmailResult[]> {
  const queries = customQueries && customQueries.length > 0 ? customQueries : [
    'subject:(invoice OR receipt OR payment) has:attachment after:2024/01/01',
    'from:(invoice OR billing OR accounts OR noreply OR no-reply) subject:(invoice) after:2024/01/01',
    'from:invoice@email.bookipi.com after:2024/01/01',
    'subject:"Invoice from" after:2024/01/01',
    'subject:(invoice) after:2024/01/01',
  ];

  const seenIds = new Set<string>();
  const invoiceEmails: InvoiceEmailResult[] = [];

  for (const query of queries) {
    let pageToken: string | undefined;

    do {
      const response = await gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 100,
        pageToken,
      });

      const messages = response.data.messages || [];
      console.log(`[Gmail:${label}] Query returned ${messages.length} results`);
      pageToken = response.data.nextPageToken || undefined;

      for (const msg of messages) {
        if (!msg.id || seenIds.has(msg.id)) continue;
        seenIds.add(msg.id);

        try {
          const detail = await gmail.users.messages.get({
            userId: 'me',
            id: msg.id,
            format: 'full',
          });

          const headers = detail.data.payload?.headers || [];
          const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value || '';
          const dateStr = headers.find(h => h.name?.toLowerCase() === 'date')?.value || '';
          const fromHeader = headers.find(h => h.name?.toLowerCase() === 'from')?.value || '';
          const snippet = detail.data.snippet || '';

          let senderEmail = '';
          let senderName = '';
          const fromMatch = fromHeader.match(/^(.+?)\s*<(.+?)>$/);
          if (fromMatch) {
            senderName = fromMatch[1].replace(/"/g, '').trim();
            senderEmail = fromMatch[2].trim();
          } else {
            senderEmail = fromHeader.trim();
          }

          let bodyText = '';
          let bodyHtml = '';
          const attachmentNames: string[] = [];

          interface PdfPart { filename: string; attachmentId: string; }
          const pdfParts: PdfPart[] = [];

          function extractParts(parts: any[]) {
            for (const part of parts) {
              if (part.mimeType === 'text/plain' && part.body?.data) {
                bodyText += Buffer.from(part.body.data, 'base64url').toString('utf8');
              }
              if (part.mimeType === 'text/html' && part.body?.data) {
                bodyHtml += Buffer.from(part.body.data, 'base64url').toString('utf8');
              }
              if (part.filename && part.filename.length > 0) {
                attachmentNames.push(part.filename);
                if (part.mimeType === 'application/pdf' && part.body?.attachmentId) {
                  pdfParts.push({ filename: part.filename, attachmentId: part.body.attachmentId });
                }
              }
              if (part.parts) {
                extractParts(part.parts);
              }
            }
          }

          const payload = detail.data.payload;
          if (payload?.parts) {
            extractParts(payload.parts);
          } else if (payload?.body?.data) {
            if (payload.mimeType === 'text/plain') {
              bodyText = Buffer.from(payload.body.data, 'base64url').toString('utf8');
            } else if (payload.mimeType === 'text/html') {
              bodyHtml = Buffer.from(payload.body.data, 'base64url').toString('utf8');
            }
          }

          const pdfTexts: string[] = [];
          for (const pdfPart of pdfParts) {
            try {
              const attachmentData = await getAttachment(gmail, msg.id, pdfPart.attachmentId);
              if (attachmentData) {
                const pdfBuffer = Buffer.from(attachmentData, 'base64');
                const pdfParseModule = await import('pdf-parse/lib/pdf-parse.js');
                const pdfParseFn = pdfParseModule.default || pdfParseModule;
                const pdfResult = await pdfParseFn(pdfBuffer);
                if (pdfResult.text) {
                  pdfTexts.push(pdfResult.text);
                }
              }
            } catch (pdfErr) {
              console.error(`[Gmail] Error parsing PDF ${pdfPart.filename} from ${msg.id}:`, pdfErr);
            }
          }

          invoiceEmails.push({
            messageId: msg.id,
            subject,
            date: dateStr,
            snippet,
            bodyText,
            bodyHtml,
            hasAttachments: attachmentNames.length > 0,
            attachmentNames,
            pdfTexts,
            senderEmail,
            senderName,
          });
        } catch (err) {
          console.error(`Error fetching invoice email ${msg.id}:`, err);
        }
      }
    } while (pageToken);
  }

  return invoiceEmails;
}

export async function searchInvoiceEmails(workspaceId: number): Promise<InvoiceEmailResult[]> {
  const clients: { client: gmail_v1.Gmail; label: string }[] = [];

  const primaryClient = await getGmailClient(workspaceId);
  if (primaryClient) clients.push({ client: primaryClient, label: 'primary' });

  const secondaryClient = await getSecondaryGmailClient(workspaceId);
  if (secondaryClient) clients.push({ client: secondaryClient, label: 'secondary' });

  if (clients.length === 0) {
    throw new Error('No Gmail account connected');
  }

  const { db } = await import('./db');
  const { workspaceInvoiceSettings } = await import('@shared/schema');
  const { eq } = await import('drizzle-orm');
  const [settings] = await db.select().from(workspaceInvoiceSettings).where(eq(workspaceInvoiceSettings.workspaceId, workspaceId));

  let customQueries: string[] | undefined;
  const searchSubject = settings?.invoiceSearchSubject?.trim();
  const searchRecipient = settings?.invoiceSearchRecipient?.trim();

  if (searchSubject || searchRecipient) {
    customQueries = [];
    if (searchSubject && searchRecipient) {
      customQueries.push(`subject:"${searchSubject}" to:${searchRecipient} after:2024/01/01`);
      customQueries.push(`subject:"${searchSubject}" after:2024/01/01`);
    } else if (searchSubject) {
      customQueries.push(`subject:"${searchSubject}" after:2024/01/01`);
    } else if (searchRecipient) {
      customQueries.push(`to:${searchRecipient} subject:(invoice) after:2024/01/01`);
    }
    customQueries.push(
      'subject:(invoice OR receipt OR payment) has:attachment after:2024/01/01',
      'from:(invoice OR billing OR accounts OR noreply OR no-reply) subject:(invoice) after:2024/01/01',
    );
    console.log(`[Gmail] Using custom invoice queries: subject="${searchSubject || ''}", recipient="${searchRecipient || ''}"`);
  }

  console.log(`[Gmail] Scanning invoices using ${clients.map(c => c.label).join(' + ')} account(s)`);

  const seenMessageIds = new Set<string>();
  const allEmails: InvoiceEmailResult[] = [];

  for (const { client, label } of clients) {
    const emails = await fetchInvoiceEmailsFromClient(client, label, customQueries);
    for (const email of emails) {
      if (!seenMessageIds.has(email.messageId)) {
        seenMessageIds.add(email.messageId);
        allEmails.push(email);
      }
    }
  }

  allEmails.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
  console.log(`[Gmail] Found ${allEmails.length} invoice-related emails across all accounts`);
  return allEmails;
}
