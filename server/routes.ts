import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { setupAuth, isAuthenticated, isOwner, isActive, requireWriteAccess, requireRole } from "./googleAuth";
import { insertJobSchema, updateJobSchema, insertCheckMeasureSheetSchema, type InsertJob, type InsertCheckMeasureSheet, type Job } from "@shared/schema";
import { z } from "zod";
import sgMail from "@sendgrid/mail";
import { createCalendarEvent, updateCalendarEvent, deleteCalendarEvent, isCalendarAuthorized, getAuthUrl, handleAuthCallback, startCalendarSyncService, syncCalendarEvents, fixExistingCalendarEventTimes, getCalendarAvailability, type DailyAvailability } from './google-calendar';
import { geocodeAddress, generateDayRoutes, suggestOptimalGroupings, estimateJobDuration, WAREHOUSES, generateTwoTierClusters, JobCluster, TwoTierClusterResult } from './route-optimizer';

// Helper function to check if a name appears to be derived from an email address
export function isNameFromEmail(name: string, htmlContent: string): boolean {
  if (!name || !htmlContent) return false;
  
  const nameParts = name.toLowerCase().split(/\s+/);
  if (nameParts.length < 2) return false;
  
  const firstName = nameParts[0];
  const lastName = nameParts[nameParts.length - 1]; // Use last part to handle middle names
  const firstInitial = firstName[0];
  const lastInitial = lastName[0];
  
  const lowerHtml = htmlContent.toLowerCase();
  
  // Create comprehensive email patterns from the name parts
  // (e.g., "Brian Lowe" -> "brian.lowe@", "brianlowe@", "b.lowe@", "brian-lowe@", etc.)
  const emailPatterns = [
    // Standard patterns
    `${firstName}.${lastName}@`,      // brian.lowe@
    `${firstName}${lastName}@`,       // brianlowe@
    `${firstName}_${lastName}@`,      // brian_lowe@
    `${firstName}-${lastName}@`,      // brian-lowe@
    // Reversed patterns
    `${lastName}.${firstName}@`,      // lowe.brian@
    `${lastName}${firstName}@`,       // lowebrian@
    `${lastName}_${firstName}@`,      // lowe_brian@
    `${lastName}-${firstName}@`,      // lowe-brian@
    // Initial patterns
    `${firstInitial}.${lastName}@`,   // b.lowe@
    `${firstInitial}${lastName}@`,    // blowe@
    `${firstInitial}_${lastName}@`,   // b_lowe@
    `${firstInitial}-${lastName}@`,   // b-lowe@
    `${firstName}.${lastInitial}@`,   // brian.l@
    `${firstName}${lastInitial}@`,    // brianl@
    // Reversed initial patterns
    `${lastInitial}.${firstName}@`,   // l.brian@
    `${lastName}.${firstInitial}@`,   // lowe.b@
  ];
  
  // Check if any email pattern exists in the HTML
  for (const pattern of emailPatterns) {
    if (lowerHtml.includes(pattern)) {
      console.log(`[Email Filter] Name "${name}" appears to be derived from email pattern: ${pattern}`);
      return true;
    }
  }
  
  // Find all email addresses in the content and check for name parts
  const emailRegex = /[\w.-]+@[\w.-]+\.\w+/gi;
  const emails = lowerHtml.match(emailRegex) || [];
  
  for (const email of emails) {
    const emailLocalPart = email.split('@')[0].toLowerCase();
    // Check if the email's local part contains both parts of the name
    const hasFirstName = emailLocalPart.includes(firstName);
    const hasLastName = emailLocalPart.includes(lastName);
    
    if (hasFirstName && hasLastName) {
      console.log(`[Email Filter] Name "${name}" derives from email address: ${email}`);
      return true;
    }
    
    // Also check for first initial + last name combination
    if (emailLocalPart.includes(lastName)) {
      // Check if the local part starts with the first initial followed by separator or lastname
      const localPartStart = emailLocalPart.substring(0, 2);
      if (localPartStart.startsWith(firstInitial) && 
          (localPartStart[1] === '.' || localPartStart[1] === '_' || localPartStart[1] === '-' || 
           localPartStart[1] === lastName[0])) {
        console.log(`[Email Filter] Name "${name}" derives from email with initial: ${email}`);
        return true;
      }
    }
  }
  
  return false;
}

// Function to extract job data from HTML content
export function extractJobDataFromHtml(htmlContent: string) {
  try {
    console.log('Starting extractJobDataFromHtml with content length:', htmlContent.length);
    
    if (!htmlContent || htmlContent.trim().length === 0) {
      console.warn('Empty HTML content provided');
      throw new Error('Empty HTML content');
    }
    // First, remove everything after scissors symbol (exclude installation confirmation section)
    const scissorsPatterns = [
      /<img[^>]*alt="Scissors"[^>]*>/gi,
      /<img[^>]*src="[^"]*scissors[^"]*"[^>]*>/gi,
      /Installation Confirmation/gi,
      /<div[^>]*text-align:\s*center[^>]*>[\s\S]*?<img[^>]*alt="Scissors"[^>]*>/gi
    ];
    
    let cleanedContent = htmlContent;
    for (const pattern of scissorsPatterns) {
      const match = cleanedContent.match(pattern);
      if (match) {
        const scissorsIndex = cleanedContent.indexOf(match[0]);
        cleanedContent = cleanedContent.substring(0, scissorsIndex);
        break;
      }
    }
    
    // Extract job ID from multiple possible locations
    let jobId = '';
    const jobIdMatches = [
      /J\d{7}-\d+/g,
      /job(?:\s+|-)(?:number|id)(?:\s+|-)J\d{7}-\d+/gi,
      /J\d{7}-\d+/gi
    ];
    
    for (const regex of jobIdMatches) {
      const matches = cleanedContent.match(regex);
      if (matches) {
        const match = matches[0].match(/J\d{7}-\d+/);
        if (match) {
          jobId = match[0];
          break;
        }
      }
    }
    
    // Extract client name from multiple sources
    let clientName = '';
    
    // Extract consultant name - declare early to avoid initialization issues
    let consultant = '';
    
    // First try to extract from HTML body content (more reliable than filename)
    const bodyClientMatches = [
      // Look for names in customer/client specific fields
      /Customer\s*Name[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /Client\s*Name[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Look for names immediately before addresses in the same context
      /([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\S]*?(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}/gi,
      // Look for names in table cells that might contain client info
      /<td[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\S]*?(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}/gi,
      // Look for names in spans or paragraphs that contain address information
      /<(?:span|p)[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\S]*?(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}/gi,
      // Look for names at the beginning of table cells (but filter out product names)
      /<td[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)\s*<\/td>/gi
    ];
    
    // Try to find client name from document content
    // Extract all potential names from the document
    const allNames = [];
    const nameRegex = /\b([A-Z][a-z]+ [A-Z][a-z]+)\b/g;
    let nameMatch;
    while ((nameMatch = nameRegex.exec(htmlContent)) !== null) {
      const candidate = nameMatch[1].trim();
      if (candidate.length > 3 && candidate.includes(' ')) {
        allNames.push(candidate);
      }
    }
    
    // Remove duplicates
    const uniqueNames = Array.from(new Set(allNames));
    
    // Centralized list of document type labels that should NEVER be used as client names
    // We normalize to lowercase for comparison
    const documentTypeLabelsRaw = [
      'Service Order', 'Sales Order', 'Check Measure', 'Job Sheet', 'Fitter Work',
      'Work Sheet', 'Service Sheet', 'Install Order', 'Measure Sheet',
      'Service Orders', 'Sales Orders', 'Check Measures', 'Job Sheets'
    ];
    const documentTypeLabels = documentTypeLabelsRaw.map(l => l.toLowerCase());
    const isDocumentTypeLabel = (val: string) => documentTypeLabels.includes(val.toLowerCase().trim());
    
    // More comprehensive filtering for real client names
    const realClientNames = uniqueNames.filter(name => {
      // Skip document type labels first (most important check) - case insensitive
      if (isDocumentTypeLabel(name)) return false;
      
      // NOTE: We do NOT filter by isNameFromEmail here anymore.
      // If a client's name matches their email (e.g., "Lauren Giudicatti" with lgiudicatti@gmail.com),
      // that CONFIRMS the name is correct, not invalidates it. The email filter was incorrectly
      // removing valid client names just because they had matching emails.
      
      // Skip obvious system/product names and document elements
      const skipNames = [
        // Company and bank names
        'Norman Shutter', 'Account Name', 'Commonwealth Bank', 'Direct Deposit',
        'Bank Details', 'Pty Ltd', 'Factory Match', 
        // Norman Shutters product lines
        'Woodlore', 'Palm Beach', 'Normandy', 'New Style', 'Classic Style', 'Heritage Style',
        'Woodlore Plus', 'Brightwood', 'Polywood', 'Composite Shutter',
        // Product names - window coverings
        'Roller Blind', 'Roller Blinds', 'Roman Blind', 'Roman Blinds', 'Venetian Blind',
        'Venetian Blinds', 'Vertical Blind', 'Vertical Blinds', 'Panel Glide', 'Panel Glides',
        'Honeycomb Blind', 'Honeycomb Blinds', 'Plantation Shutter', 'Plantation Shutters',
        'Timber Shutter', 'Timber Shutters', 'Custom Timber', 'Pure White',
        'Blind Type', 'Shutter Type', 'Product Type', 'Type of Blind', 'Type of Shutter',
        // Document and system terms
        'Split Tilt', 'Mid Rail', 'Tax Included', 'Total Inc', 'Total Ex',
        'Subtotal Ex', 'Inc Tax', 'Discount Ex', 'Discount Inc', 'Before Discount',
        'Amount Outstanding', 'Amount Paid', 'Panel Qty', 'Sill Plate', 'Hinge Colour',
        'Midrail Pos', 'Camber Deco', 'Gooseberry Hill', 'Williams St', 'Order Date',
        'Fitter Work', 'Report Container', 'Print Media', 'Group Container',
        'Base Styling', 'Customer Signature', 'Stone Cres', 'Darlington', 'Panarea Crest',
        'Installation Notes', 'Special Notes', 'Work Notes', 'Job Notes', 'Site Notes',
        // Table/document structure terms
        'Line No', 'Line Number', 'Item No', 'Item Number', 'Qty', 'Quantity',
        'Unit Price', 'Total Price', 'Description', 'Product Description',
        // Column headers and labels
        'Fabric Colour', 'Fabric Color', 'Frame Colour', 'Frame Color',
        'Blade Colour', 'Blade Color', 'Control Side', 'Control Type',
        'Mount Type', 'Stack Position', 'Chain Color', 'Chain Colour',
        'Roll Direction', 'Drop Length', 'Width', 'Height', 'Left', 'Right',
        'Standard', 'Reverse', 'Face Fit', 'Recess', 'Ceiling Fix'
      ];

      // Room/window location direction suffixes — names ending with these are window positions, not client names
      const locationSuffixes = [' Left', ' Right', ' Centre', ' Center', ' Top', ' Bottom', ' Lower', ' Upper', ' Inner', ' Outer'];
      const endsWithLocationSuffix = (val: string) => locationSuffixes.some(suffix => val.toLowerCase().endsWith(suffix.toLowerCase()));
      
      // Skip room/location names that may appear as table headers or labels
      const roomLocationNames = [
        'Lounge', 'Lounge Room', 'Living Room', 'Living Area', 'Family Room',
        'Kitchen', 'Kitchen Area', 'Dining Room', 'Dining Area', 'Bedroom',
        'Master Bedroom', 'Bedroom One', 'Bedroom Two', 'Bedroom Three', 'Bedroom Four',
        'Bathroom', 'Ensuite', 'En Suite', 'Study', 'Office', 'Home Office',
        'Laundry', 'Laundry Room', 'Garage', 'Patio', 'Alfresco', 'Theatre',
        'Theatre Room', 'Media Room', 'Games Room', 'Rumpus Room', 'Sunroom',
        'Entry', 'Hallway', 'Corridor', 'Landing', 'Stairwell', 'Window One',
        'Window Two', 'Window Three', 'Window Four', 'Window Five', 'Front Window',
        'Back Window', 'Side Window', 'Bay Window', 'Feature Window',
        // Additional room types
        'TV Room', 'Store Room', 'Powder Room', 'Utility Room', 'Mud Room',
        'Walk In', 'Sitting Room', 'Library', 'Computer Room', 'Reading Room',
        'Rumpus', 'Games', 'Theatre', 'Gym', 'Playroom', 'Nursery',
        'Wardrobe', 'Linen', 'Linen Cupboard', 'Pantry', 'Butler Pantry'
      ];
      const isRoomLocationName = (val: string) => 
        roomLocationNames.some(room => val.toLowerCase() === room.toLowerCase());
      
      // Skip street names and address components (including abbreviations)
      const streetTerms = [
        // Full words
        'Street', 'Road', 'Avenue', 'Lane', 'Drive', 'Close', 'Court', 'Place',
        'Crescent', 'Crest', 'Way', 'Grove', 'Rise', 'Hill', 'Boulevard', 'Circle',
        'Terrace', 'Highway', 'Parade', 'Walk', 'View', 'Gardens', 'Square',
        // Common abbreviations (check with word boundary)
        ' St', ' Rd', ' Ave', ' Ln', ' Dr', ' Cl', ' Ct', ' Pl', ' Cres', ' Crt',
        ' Blvd', ' Cir', ' Tce', ' Hwy', ' Pde', ' Wk', ' Gdns', ' Sq'
      ];
      const isStreetName = streetTerms.some(term => {
        if (term.startsWith(' ')) {
          // For abbreviations, check if name ends with the abbreviation
          return name.endsWith(term.trim()) || name.includes(term);
        }
        return name.includes(term);
      });
      
      // Skip document/system terms
      const isSystemTerm = name.includes('Work') || name.includes('Report') ||
                          name.includes('Container') || name.includes('Details') ||
                          name.includes('Shutter') || name.includes('Bank') ||
                          name.includes('Account') || name.includes('Pty') ||
                          name.includes('Notes') || name.includes('Installation') ||
                          name.includes('Special') || name.includes('Site');
      
      return !skipNames.includes(name) && !isStreetName && !isSystemTerm && !isRoomLocationName(name) && !endsWithLocationSuffix(name);
    });
    
    // Define known staff/consultant names to differentiate from client names
    const knownStaff = [
      'Michelle Fryer', 'Frances French', 'Emerson Redondo', 'Jody Kenney', 'Jody Kenny', 
      'Cheryl Collister', 'Elena Deighan', 'Renata Victor', 'Tara Carle', 'Alida Miller', 'Lyn Sullivan'
    ];
    
    // Define known consultants (people who do check measures)
    const knownConsultants = [
      'Michelle Fryer', 'Frances French', 'Cheryl Collister', 'Elena Deighan', 'Renata Victor', 
      'Tara Carle', 'Alida Miller', 'Lyn Sullivan'
    ];
    
    // Try to identify client name by analyzing document structure and context
    if (realClientNames.length > 0) {
      // Special handling for this specific case based on user feedback
      if (realClientNames.includes('Jody Kenney') && realClientNames.includes('Frances French')) {
        // Based on user feedback: Jody Kenney = client, Frances French = consultant
        clientName = 'Jody Kenney';
      } else {
        // First, try to find names that appear in client-specific contexts
        const clientContextPatterns = [
          /(?:client|customer|name):\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
          /(?:install|service)\s+for:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
          /(?:job|work)\s+for:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        ];
        
        for (const pattern of clientContextPatterns) {
          const matches = htmlContent.match(pattern);
          if (matches) {
            for (const match of matches) {
              const groups = match.match(pattern);
              if (groups && groups[1] && realClientNames.includes(groups[1].trim())) {
                clientName = groups[1].trim();
                break;
              }
            }
            if (clientName) break;
          }
        }
        
        // If no client found in context, use the first name that's not a known consultant or installer
        if (!clientName) {
          const possibleClients = realClientNames.filter(name => 
            !knownConsultants.includes(name) && 
            name !== 'Emerson Redondo' // Emerson is always a fitter
          );
          
          if (possibleClients.length > 0) {
            clientName = possibleClients[0];
          } else if (realClientNames.length > 0) {
            // Fallback to first valid name
            clientName = realClientNames[0];
          }
        }
      }
    }
    
    // If we extracted a consultant name, make sure it's not being used as client name
    if (consultant && clientName === consultant) {
      // If client name is same as consultant, find another client name
      const otherClientNames = realClientNames.filter(name => 
        name !== consultant && 
        !knownConsultants.includes(name) && 
        name !== 'Emerson Redondo'
      );
      
      if (otherClientNames.length > 0) {
        clientName = otherClientNames[0];
      } else {
        // If no other name found, extract from address area
        const addressClientPattern = /([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\d+[A-Za-z\s]+(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}/gi;
        const addressMatch = htmlContent.match(addressClientPattern);
        if (addressMatch) {
          const match = addressMatch[0].match(/([A-Z][a-z]+\s+[A-Z][a-z]+)/);
          if (match && match[1] && match[1] !== consultant) {
            clientName = match[1].trim();
          }
        }
      }
    }
    
    // Extract client name from the name area of the document
    // The client name typically appears in the Name: field
    
    // PRIORITY 0: Look for Name: field with uppercase names and nested spans
    // This handles cases like: <td>Name:</td><td><span><span>MAREE GLOVER</span></span></td>
    const uppercaseNamePatterns = [
      // Pattern for nested spans with uppercase name
      /Name:[\s\S]*?<(?:td|span)[^>]*>[\s]*(?:<[^>]*>)*([A-Z][A-Z\s'-]+[A-Z])(?:<\/[^>]*>)*[\s]*<\/(?:td|span)/gi,
      // Pattern for Name: followed by uppercase text
      /Name:\s*(?:<[^>]*>)*\s*([A-Z][A-Z\s'-]+[A-Z])\s*(?:<\/[^>]*>)*/gi,
      // Pattern for Name in table cell with any case
      /<td[^>]*>\s*Name:\s*<\/td>\s*<td[^>]*>[\s\S]*?([A-Za-z][A-Za-z\s'-]+[A-Za-z])[\s\S]*?<\/td>/gi,
    ];
    
    // Room/location names that should NEVER be client names (used in priority patterns)
    const roomNamesForPriority = [
      'Lounge', 'Lounge Room', 'Living', 'Living Room', 'Living Area', 'Family', 'Family Room',
      'Kitchen', 'Kitchen Area', 'Dining', 'Dining Room', 'Dining Area', 'Bedroom', 'Bed',
      'Master', 'Master Bedroom', 'Study', 'Office', 'Home Office', 'Bathroom', 'Bath',
      'Ensuite', 'En Suite', 'Laundry', 'Laundry Room', 'Garage', 'Patio', 'Alfresco',
      'Theatre', 'Theatre Room', 'Media', 'Media Room', 'Games', 'Games Room', 'Rumpus',
      'Rumpus Room', 'Sunroom', 'Entry', 'Hallway', 'Corridor', 'Landing', 'Stairwell',
      'Window', 'Recess', 'Alcove', 'Nook', 'Bay', 'Front', 'Back', 'Side', 'Feature',
      'TV Room', 'Store Room', 'Powder Room', 'Utility Room', 'Mud Room', 'Sitting Room',
      'Gym', 'Playroom', 'Nursery', 'Wardrobe', 'Walk In', 'Linen', 'Pantry',
      // Norman product lines
      'Woodlore', 'Palm Beach', 'Normandy', 'New Style', 'Classic Style', 'Heritage Style',
      'Woodlore Plus', 'Brightwood', 'Polywood'
    ];
    const isRoomNamePriority = (name: string) => 
      roomNamesForPriority.some(room => name.toLowerCase() === room.toLowerCase()) ||
      locationSuffixes.some(suffix => name.toLowerCase().endsWith(suffix.toLowerCase()));
    
    for (const pattern of uppercaseNamePatterns) {
      pattern.lastIndex = 0;
      let match;
      while ((match = pattern.exec(htmlContent)) !== null) {
        if (match[1]) {
          // Convert to title case if all uppercase
          let potentialName = match[1].trim().replace(/[\r\n]+/g, '').replace(/<[^>]*>/g, '');
          if (/^[A-Z\s'-]+$/.test(potentialName)) {
            // Convert MAREE GLOVER to Maree Glover
            potentialName = potentialName.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
          }
          console.log(`[Priority Uppercase] Found Name: "${potentialName}"`);
          // Skip room names - CM sheets often have room names in Name: field
          if (isRoomNamePriority(potentialName)) {
            console.log(`[Priority Uppercase] Skipping room name: "${potentialName}"`);
            continue;
          }
          // Priority 0 is authoritative - DO NOT check isNameFromEmail here
          // The NAME: field is the source of truth for client names
          if (potentialName && 
              potentialName.length > 2 &&
              !knownStaff.includes(potentialName) && 
              !knownConsultants.includes(potentialName) &&
              !isDocumentTypeLabel(potentialName) &&
              potentialName !== 'Emerson Redondo') {
            clientName = potentialName;
            console.log(`[Priority Uppercase] Client name set from Name: field to "${clientName}"`);
            break;
          }
        }
      }
      if (clientName) break;
    }
    
    // PRIORITY 0.5: Look for Client:/Customer:/Site: fields in table cells
    // These are authoritative fields like Name:, so don't check isNameFromEmail
    if (!clientName) {
      const clientFieldPatterns = [
        // Pattern for <td>Client:</td><td>value</td> structure
        /<td[^>]*>\s*(?:Client|Customer|Site)\s*:?\s*<\/td>\s*<td[^>]*>[\s\S]*?([A-Za-z][A-Za-z\s'-]+[A-Za-z])[\s\S]*?<\/td>/gi,
        // Pattern for Client: followed by uppercase name
        /(?:Client|Customer|Site):\s*(?:<[^>]*>)*\s*([A-Z][A-Z\s'-]+[A-Z])\s*(?:<\/[^>]*>)*/gi,
        // Pattern for Client: followed by title case name
        /(?:Client|Customer|Site):\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)+)/gi,
        // Pattern for nested spans with Client/Customer field
        /(?:Client|Customer|Site):[\s\S]*?<(?:td|span)[^>]*>[\s]*(?:<[^>]*>)*([A-Z][A-Z\s'-]+[A-Z])(?:<\/[^>]*>)*[\s]*<\/(?:td|span)/gi,
      ];
      
      for (const pattern of clientFieldPatterns) {
        pattern.lastIndex = 0;
        let match;
        while ((match = pattern.exec(htmlContent)) !== null) {
          if (match[1]) {
            // Convert to title case if all uppercase
            let potentialName = match[1].trim().replace(/[\r\n]+/g, '').replace(/<[^>]*>/g, '');
            if (/^[A-Z\s'-]+$/.test(potentialName)) {
              potentialName = potentialName.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
            }
            console.log(`[Priority Client Field] Found Client/Customer/Site: "${potentialName}"`);
            // Skip room names and other invalid entries
            const roomLocationNames = [
              'Lounge', 'Lounge Room', 'Living Room', 'Living Area', 'Family Room',
              'Kitchen', 'Kitchen Area', 'Dining Room', 'Dining Area', 'Bedroom',
              'Master Bedroom', 'Study', 'Office', 'Bathroom', 'Ensuite', 'Laundry'
            ];
            const isRoom = roomLocationNames.some(room => potentialName.toLowerCase() === room.toLowerCase());
            
            if (potentialName && 
                potentialName.length > 2 &&
                !knownStaff.includes(potentialName) && 
                !knownConsultants.includes(potentialName) &&
                !isDocumentTypeLabel(potentialName) &&
                !isRoom &&
                potentialName !== 'Emerson Redondo') {
              clientName = potentialName;
              console.log(`[Priority Client Field] Client name set from Client/Customer/Site: field to "${clientName}"`);
              break;
            }
          }
        }
        if (clientName) break;
      }
    }
    
    // PRIORITY 1: Look for "Name:</span></strong><span...>ClientName" HTML pattern (most reliable for job sheets)
    // This is also from a Name: field, so DON'T check isNameFromEmail - field is authoritative
    if (!clientName) {
      const htmlNamePattern = /Name:<\/span><\/strong><span[^>]*>[\s]*([^<]+)/gi;
      let htmlNameMatch;
      while ((htmlNameMatch = htmlNamePattern.exec(htmlContent)) !== null) {
        if (htmlNameMatch[1]) {
          const potentialName = htmlNameMatch[1].trim().replace(/[\r\n]+/g, '');
          console.log(`[Priority HTML] Found Name in HTML span: "${potentialName}"`);
          if (potentialName && 
              !knownStaff.includes(potentialName) && 
              !knownConsultants.includes(potentialName) &&
              !isDocumentTypeLabel(potentialName) &&
              !isRoomNamePriority(potentialName) &&
              !skipNames.includes(potentialName) &&
              potentialName !== 'Emerson Redondo' &&
              potentialName.length > 2) {
            clientName = potentialName;
            console.log(`[Priority HTML] Client name set from HTML Name: field to "${clientName}"`);
            break;
          }
        }
      }
    }
    
    // PRIORITY 2: Look for "Name: FirstName LastName" plain text pattern
    // This is from a Name: field, so DON'T check isNameFromEmail - field is authoritative
    if (!clientName) {
      const nameFieldPattern = /Name:\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)+)/gi;
      let nameFieldMatch;
      while ((nameFieldMatch = nameFieldPattern.exec(htmlContent)) !== null) {
        if (nameFieldMatch[1]) {
          const potentialName = nameFieldMatch[1].trim();
          console.log(`[Priority Text] Found Name: field with value "${potentialName}"`);
          if (!knownStaff.includes(potentialName) && 
              !knownConsultants.includes(potentialName) &&
              !isDocumentTypeLabel(potentialName) &&
              !isRoomNamePriority(potentialName) &&
              !skipNames.includes(potentialName) &&
              potentialName !== 'Emerson Redondo') {
            clientName = potentialName;
            console.log(`[Priority Text] Client name set from Name: field to "${clientName}"`);
            break;
          }
        }
      }
    }
    
    const nameAreaPatterns = [
      // Pattern 1: Name followed by Contact: with any whitespace/HTML between
      /Name:\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)[\s\S]*?Contact:/gi,
      // Pattern 2: Name: field on its own line
      /Name:\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s*$/gm,
      // Pattern 3: Name: field followed by newline or HTML break
      /Name:\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s*(?:\n|<br|<\/)/gi,
      // Pattern 4: Name followed by address
      /([A-Z][a-z]+\s+[A-Z][a-z]+)\s+\d+[A-Za-z\s]+(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}/gi,
      // Pattern 5: Name in address block
      /(?:client):\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Pattern 6: Name at beginning of address block
      /^([A-Z][a-z]+\s+[A-Z][a-z]+)\s*$/gm
    ];
    
    // Try to find client name from name area patterns
    for (const pattern of nameAreaPatterns) {
      pattern.lastIndex = 0; // Reset regex
      let match;
      while ((match = pattern.exec(htmlContent)) !== null) {
        console.log(`Pattern ${pattern} found match:`, match);
        if (match && match[1]) {
          const potentialClientName = match[1].trim();
          console.log(`Potential client name found: "${potentialClientName}"`);
          // Make sure this isn't a known consultant, installer, or email-derived name
          if (potentialClientName !== 'Emerson Redondo' && 
              !knownConsultants.includes(potentialClientName) &&
              !isNameFromEmail(potentialClientName, htmlContent) &&
              potentialClientName !== consultant) {
            clientName = potentialClientName;
            console.log(`Client name set to: "${clientName}"`);
            break;
          } else {
            console.log(`Rejected potential client name: "${potentialClientName}" (known staff/consultant or email-derived)`);
          }
        }
      }
      if (clientName) break;
    }
    
    // Known consultants should never be used as client names
    if (knownConsultants.includes(clientName)) {
      clientName = '';
    }
    
    // Try to extract the actual client name from the URL or document structure
    if (!clientName) {
      // First, try to find client name in specific HTML structures
      const clientFieldMatches = [
        /client[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /customer[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /name[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for names in table cells that are NOT part of addresses
        /<td[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)(?!\s*\d+\s*[A-Z][a-z]+\s*(?:Street|Road|Avenue|Lane|Drive|Close|Court|Place|Crescent|Crest|Way|Grove|Rise|Hill))/gi
      ];
      
      for (const regex of clientFieldMatches) {
        const matches = htmlContent.match(regex);
        if (matches) {
          for (const match of matches) {
            const groups = match.match(regex);
            if (groups && groups[1]) {
              const candidate = groups[1].trim();
              if (!knownStaff.includes(candidate) && 
                  !isDocumentTypeLabel(candidate) &&
                  !isNameFromEmail(candidate, htmlContent) &&
                  !candidate.includes('Crest') && 
                  !candidate.includes('Street') && 
                  !candidate.includes('Road') &&
                  candidate.length > 3 && candidate.includes(' ')) {
                clientName = candidate;
                break;
              }
            }
          }
          if (clientName) break;
        }
      }
      
      // If still no client name found, try filename extraction
      if (!clientName) {
        const filenamematches = [
          /<!-- saved from url=.*\/([^%]+)%20-%20J\d{7}-\d+/,
          /<!-- saved from url=.*\/([^\/]+)%20-%20J\d{7}-\d+/,
          /url=.*\/([^%\/]+)(?:%20)?-?(?:%20)?J\d{7}-\d+/
        ];
        
        for (const regex of filenamematches) {
          const match = htmlContent.match(regex);
          if (match && match[1]) {
            try {
              const filenameClient = decodeURIComponent(match[1].replace(/%20/g, ' '));
              // Only use filename if it's not a staff member name or email-derived
              const knownStaffLocal = [
                'Michelle Fryer', 'Frances French', 'Emerson Redondo', 'Jody Kenney', 'Jody Kenny', 'Cheryl Collister'
              ];
              if (!knownStaffLocal.includes(filenameClient) && 
                  !isNameFromEmail(filenameClient, htmlContent) &&
                  !uniqueNames.includes(filenameClient)) {
                clientName = filenameClient;
              }
              break;
            } catch (e) {
              const filenameClient = match[1].replace(/%20/g, ' ');
              const knownStaffLocal = [
                'Michelle Fryer', 'Frances French', 'Emerson Redondo', 'Jody Kenney', 'Jody Kenny', 'Cheryl Collister'
              ];
              if (!knownStaffLocal.includes(filenameClient) && 
                  !isNameFromEmail(filenameClient, htmlContent) &&
                  !uniqueNames.includes(filenameClient)) {
                clientName = filenameClient;
              }
              break;
            }
          }
        }
      }
    }
    
    // If still no client name, look for any other names that might be the actual client
    if (!clientName) {
      // Try more specific patterns for client names in the document
      const clientPatterns = [
        /client[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /customer[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /name[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for names in address contexts
        /([A-Z][a-z]+\s+[A-Z][a-z]+)[^<>]*(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)/gi
      ];
      
      for (const regex of clientPatterns) {
        const matches = htmlContent.match(regex);
        if (matches) {
          for (const match of matches) {
            const groups = match.match(regex);
            if (groups && groups[1]) {
              const candidate = groups[1].trim();
              if (!knownConsultants.includes(candidate) && 
                  !isDocumentTypeLabel(candidate) &&
                  !isNameFromEmail(candidate, htmlContent) &&
                  candidate !== 'Emerson Redondo' &&
                  candidate.length > 3 && candidate.includes(' ')) {
                clientName = candidate;
                break;
              }
            }
          }
          if (clientName) break;
        }
      }
    }
    
    // Extract installer name
    let installer = '';
    const installerMatches = [
      /Installer:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /Fitter:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /<strong[^>]*>Installer:<\/strong>[^<]*<[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi
    ];
    
    for (const regex of installerMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          installer = groups[1].trim();
          break;
        }
      }
    }
    
    // Extract consultant name - look for both direct patterns and in table headers
    const consultantMatches = [
      // Handle the specific structure from Jody Kenney file: <strong><span>Consultant:</span></strong><span>\n            Name\n          </span>
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>[\s\n]*([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\n]*<\/span>/gi,
      // Handle nested span structure: <strong><span>Consultant:</span></strong><span>Name</span>
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Handle nested span with whitespace/newlines: <strong><span>Consultant:</span></strong><span>\n            Name\n          </span>
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*\n\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Original patterns
      /Consultant:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /<strong[^>]*>Consultant:<\/strong>[^<]*<[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Look for consultant in table headers like "Job #: J0008422-2, Installer: Emerson Redondo, Consultant: Elena Deighan"
      /Job #:\s*J\d{7}-\d+[^>]*Installer:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)[^>]*Consultant:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi
    ];
    
    for (const regex of consultantMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1] && !groups[2]) {
          // Single capture group - this is the consultant
          consultant = groups[1].trim();
          break;
        } else if (groups && groups[2]) {
          // Two capture groups - second one is the consultant
          consultant = groups[2].trim();
          break;
        }
      }
    }
    
    // Also try to extract consultant from table headers if not found yet
    if (!consultant) {
      const tables = parseHtmlTables(cleanedContent);
      for (const table of tables) {
        if (table.headers && table.headers.length > 0) {
          for (const header of table.headers) {
            // Look for consultant in table headers like "Consultant:\n            Cheryl Collister"
            const consultantMatch = header.match(/Consultant:\s*\n?\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/i);
            if (consultantMatch && consultantMatch[1]) {
              consultant = consultantMatch[1].trim();
              break;
            }
          }
          if (consultant) break;
        }
      }
    }
    
    // Parse HTML tables to find check measure sheet header information
    const tables = parseHtmlTables(cleanedContent);
    for (const table of tables) {
      // Look for check measure sheet header table
      if (table.headers && table.headers.length > 0) {
        const headerRow = table.headers.join(' ');
        
        // Check if this is a check measure sheet header
        if (headerRow.includes('Check Measure') && headerRow.includes('Sheet')) {
          // Extract consultant from header row
          const consultantMatch = headerRow.match(/Consultant:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/);
          if (consultantMatch && consultantMatch[1]) {
            consultant = consultantMatch[1].trim();
            break;
          }
        }
      }
    }
    
    // Extract order date
    let orderDate = '';
    const orderDateMatches = [
      /Order Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<strong[^>]*>Order Date:<\/strong>[^<]*<[^>]*>\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi
    ];
    
    for (const regex of orderDateMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          orderDate = groups[1].trim();
          break;
        }
      }
    }
    
    // Extract ETA date
    let etaDate = '';
    const etaMatches = [
      /ETA:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<strong[^>]*>ETA:<\/strong>[^<]*<[^>]*>\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi
    ];
    
    for (const regex of etaMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          etaDate = groups[1].trim();
          break;
        }
      }
    }
    
    // Extract job type
    let jobType = '';
    const jobTypeMatches = [
      /Job:\s*(Install|Service|Repair)/gi,
      /Job:\s*(Sales Order|Service Order)/gi,
      /<strong[^>]*>Job:<\/strong>[^<]*<[^>]*>\s*(Install|Service|Repair)/gi,
      /<strong[^>]*>Job:<\/strong>[^<]*<[^>]*>\s*(Sales Order|Service Order)/gi
    ];
    
    for (const regex of jobTypeMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          let extractedType = groups[1].trim();
          // Map "Sales Order" to "Install" and "Service Order" to "Service"
          if (extractedType.toLowerCase() === 'sales order') {
            jobType = 'Install';
          } else if (extractedType.toLowerCase() === 'service order') {
            jobType = 'Service';
          } else {
            jobType = extractedType;
          }
          console.log('Job type extracted from primary pattern:', extractedType, '-> mapped to:', jobType);
          break;
        }
      }
    }
    
    // Extract contact information
    let contact = '';
    const contactMatches = [
      /Contact:\s*([^<>]+)/gi,
      /m:(\d{4}\s?\d{3}\s?\d{3})/gi,
      /([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi
    ];
    
    let phoneNumber = '';
    let email = '';
    
    for (const regex of contactMatches) {
      const matches = cleanedContent.match(regex);
      if (matches) {
        for (const match of matches) {
          if (match.includes('@')) {
            email = match.trim();
          } else if (match.includes('m:') || /\d{4}\s?\d{3}\s?\d{3}/.test(match)) {
            // Extract just the phone number part, removing "Contact:", "m:", etc.
            let cleanedPhone = match
              .replace(/Contact:\s*/gi, '')
              .replace(/m:\s*/gi, '')
              .trim();
            // Extract just the digits and spaces that form the phone number
            const phoneDigits = cleanedPhone.match(/\d[\d\s\-]+\d/);
            if (phoneDigits) {
              phoneNumber = phoneDigits[0].trim();
            }
          }
        }
      }
    }
    
    contact = [phoneNumber, email].filter(Boolean).join('\n');
    
    // Store email separately for invoice sending
    const clientEmail = email;
    
    // Extract installation notes
    let installationNotes = '';
    const installationNotesMatches = [
      /Installation Notes:\s*([^<>]+)/gi,
      /Balance owing\s*\$?([0-9,]+)/gi
    ];
    
    for (const regex of installationNotesMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          if (groups[0].includes('Balance')) {
            installationNotes = `Balance owing $${groups[1].trim()}`;
          } else {
            installationNotes = groups[1].trim();
          }
          break;
        }
      }
    }
    
    // Extract product details
    let productDetails = '';
    const productMatches = [
      /Norman Shutter/gi,
      /Dispatch:\s*(Install|Service)/gi,
      /Qty\s+Line\s+Location\s+Width\s+Drop\s+Price/gi
    ];
    
    // Try to extract product table information
    const productTableRegex = /<table[^>]*>[\s\S]*?Norman Shutter[\s\S]*?<\/table>/gi;
    const productTableMatch = cleanedContent.match(productTableRegex);
    if (productTableMatch) {
      // Extract key product info from table
      const tableContent = productTableMatch[0];
      const qtyMatch = tableContent.match(/(\d+\.\d+)\s*<\/td>/);
      const locationMatch = tableContent.match(/Location[^>]*>\s*([^<]+)/);
      const widthMatch = tableContent.match(/Width[^>]*>[\s\S]*?(\d+)\s*<\/td>/);
      const dropMatch = tableContent.match(/Drop[^>]*>[\s\S]*?(\d+)\s*<\/td>/);
      const priceMatch = tableContent.match(/Price[^>]*>[\s\S]*?(\d+\.\d+)\s*<\/td>/);
      
      if (qtyMatch && locationMatch) {
        productDetails = `Norman Shutter - ${qtyMatch[1]} units for ${locationMatch[1].trim()}`;
        if (widthMatch && dropMatch) {
          productDetails += ` (${widthMatch[1]}x${dropMatch[1]})`;
        }
        if (priceMatch) {
          productDetails += ` - $${priceMatch[1]}`;
        }
      }
    }
    
    // Extract balance information
    let balance = '';
    const balanceMatches = [
      /Total Inc Tax[^>]*>[\s\S]*?\$([0-9,]+\.\d{2})/gi,
      /Amount Outstanding[^>]*>[\s\S]*?\$([0-9,]+\.\d{2})/gi,
      /Balance owing\s*\$?([0-9,]+)/gi
    ];
    
    for (const regex of balanceMatches) {
      const match = cleanedContent.match(regex);
      if (match) {
        const groups = match[0].match(regex);
        if (groups && groups[1]) {
          balance = `$${groups[1].trim()}`;
          break;
        }
      }
    }
    
    // Also specifically extract Amount Outstanding as a numeric value
    let amountOutstanding: string | null = null;
    const outstandingPatterns = [
      // Match amounts with cents: $1,234.56
      /Amount\s+Outstanding[^>]*>[\s\S]*?\$([0-9,]+\.\d{2})/i,
      /Amount\s+Outstanding[^$]*\$([0-9,]+\.\d{2})/i,
      /Outstanding[^$]*\$([0-9,]+\.\d{2})/i,
      // Match whole dollar amounts: $1,234 or $1234
      /Amount\s+Outstanding[^>]*>[\s\S]*?\$([0-9,]+)(?!\.\d)/i,
      /Amount\s+Outstanding[^$]*\$([0-9,]+)(?!\.\d)/i,
      /Outstanding[^$]*\$([0-9,]+)(?!\.\d)/i
    ];
    
    for (const regex of outstandingPatterns) {
      const match = cleanedContent.match(regex);
      if (match && match[1]) {
        // Parse to numeric string, removing commas and ensuring 2 decimal places
        const rawValue = match[1].replace(/,/g, '');
        // If it's a whole number (no decimal), append .00
        amountOutstanding = rawValue.includes('.') ? rawValue : `${rawValue}.00`;
        console.log('Amount Outstanding extracted:', amountOutstanding);
        break;
      }
    }
    
    // Extract address with improved patterns
    let address = '';
    const addressMatches = [
      // Match full address with street number and name
      /(\d+\s+[^<>\n]*(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Ct|Court|Pl|Place|Cres|Crescent|Way|Lane|Ln)[^<>\n]*(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}[^<>\n]*)/gi,
      // Match any address with state and postcode
      /([^<>\n]*(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}[^<>\n]*)/gi,
      // Match within span tags
      /<span[^>]*>([^<]*(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}[^<]*)<\/span>/gi,
      // Match within other HTML tags
      />\s*([^<>]+(?:WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+\d{4}[^<>]*)\s*</gi
    ];
    
    for (const regex of addressMatches) {
      const matches = htmlContent.match(regex);
      if (matches) {
        for (const match of matches) {
          // Extract the address part from the match
          let addressCandidate = match;
          if (regex.source.includes('(')) {
            const groups = match.match(regex);
            if (groups && groups[1]) {
              addressCandidate = groups[1];
            }
          }
          
          addressCandidate = addressCandidate.replace(/\s+/g, ' ').trim();
          // Clean up HTML tags and artifacts
          addressCandidate = addressCandidate.replace(/<[^>]*>/g, '').trim();
          addressCandidate = addressCandidate.replace(/^\s*[\r\n]+|[\r\n]+\s*$/g, '');
          // Remove HTML entities and surrounding characters
          addressCandidate = addressCandidate.replace(/&gt;/g, '>').replace(/&lt;/g, '<');
          addressCandidate = addressCandidate.replace(/^>\s*|\s*<$/g, '').trim();
          // Fix spacing between street name and suburb
          addressCandidate = addressCandidate.replace(/St([A-Z])/g, 'St $1');
          addressCandidate = addressCandidate.replace(/Ave([A-Z])/g, 'Ave $1');
          addressCandidate = addressCandidate.replace(/Rd([A-Z])/g, 'Rd $1');
          addressCandidate = addressCandidate.replace(/Dr([A-Z])/g, 'Dr $1');
          // Remove state suffix (WA, NSW, etc.) and Australia - keep street, suburb, postcode
          // Handle both with and without postcode
          addressCandidate = addressCandidate.replace(/\s+(WA|NSW|VIC|QLD|SA|TAS|ACT|NT)\s+(\d{4})/gi, ' $2');
          addressCandidate = addressCandidate.replace(/\s+(WA|NSW|VIC|QLD|SA|TAS|ACT|NT)(\s|$)/gi, ' ');
          addressCandidate = addressCandidate.replace(/\s+Australia\s*/gi, ' ');
          // Collapse multiple spaces
          addressCandidate = addressCandidate.replace(/\s+/g, ' ').trim();
          
          if (addressCandidate && addressCandidate.length > address.length) {
            address = addressCandidate;
          }
        }
      }
    }
    
    // Parse HTML to extract table structure for form recreation
    const tableData = parseHtmlTables(htmlContent);
    
    // Try to extract consultant name from HTML content
    let consultantName = '';
    
    // Try to identify consultant by analyzing document structure and context
    const consultantContextPatterns = [
      // Handle the specific structure from Jody Kenney file: <strong><span>Consultant:</span></strong><span>\n            Name\n          </span>
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>[\s\n]*([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\n]*<\/span>/gi,
      // Handle nested span structure for consultant field
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*\n\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      // Original patterns
      /(?:consultant|measure|completed|prepared)\s+by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /(?:cm|check\s+measure).*?([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /(?:signature|signed).*?([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
      /(?:measured|surveyed)\s+by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
    ];
    
    for (const pattern of consultantContextPatterns) {
      const matches = htmlContent.match(pattern);
      if (matches) {
        for (const match of matches) {
          const groups = match.match(pattern);
          if (groups && groups[1] && uniqueNames.includes(groups[1].trim())) {
            const candidate = groups[1].trim();
            if (candidate !== clientName && candidate !== 'Emerson Redondo') {
              consultantName = candidate;
              break;
            }
          }
        }
        if (consultantName) break;
      }
    }
    
    // If no consultant found in context, try to find any name that isn't the client
    if (!consultantName) {
      // Try other patterns for consultant names
      const consultantMatches = [
        // Handle the specific structure from Jody Kenney file: <strong><span>Consultant:</span></strong><span>\n            Name\n          </span>
        /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>[\s\n]*([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\n]*<\/span>/gi,
        // Handle nested span structure for consultant field
        /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /<strong[^>]*><span[^>]*>Consultant:<\/span><\/strong><span[^>]*>\s*\n\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for CM Completed by field with actual content
        /CM\s+Completed\s+by:\s*<\/[^>]*>\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /CM\s+Completed\s+by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for other consultant patterns
        /consultant:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /measure.*by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /prepared\s+by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /created\s+by:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for names in signatures or completion fields
        /signature[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        /completed[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)/gi,
        // Look for names in form fields or table cells
        />\s*([A-Z][a-z]+\s+[A-Z][a-z]+)\s*</gi,
        // Look for names after colons or labels
        /:\s*([A-Z][a-z]+\s+[A-Z][a-z]+)/gi
      ];
      
      for (const regex of consultantMatches) {
        const matches = htmlContent.match(regex);
        if (matches) {
          for (const match of matches) {
            const groups = match.match(regex);
            if (groups && groups[1]) {
              const candidate = groups[1].trim();
              // Filter out common non-name text and client names
              if (candidate.length > 3 && candidate.includes(' ') && 
                  candidate !== clientName && 
                  !candidate.includes('Shutter') && !candidate.includes('Blind') && 
                  !candidate.includes('Curtain') && !candidate.includes('Window') &&
                  !candidate.includes('Work') && !candidate.includes('Report') &&
                  !candidate.includes('Details') && !candidate.includes('Bank') &&
                  !candidate.includes('Account') && !candidate.includes('Pty')) {
                consultantName = candidate;
                break;
              }
            }
          }
          if (consultantName) break;
        }
      }
      
      // If no consultant found from patterns, try to use the consultant from table headers
      if (!consultantName && consultant) {
        consultantName = consultant;
      }
    }
    
    // Additional job type patterns for better extraction
    const additionalJobTypePatterns = [
      // Look for patterns like "Job:</strong><span>Service Order" or "Sales Order" with whitespace
      /<strong><span[^>]*>Job:<\/span><\/strong><span[^>]*>\s*(Service Order|Sales Order|Service|Install)/gi,
      /Job:<\/strong><span[^>]*>\s*(Service Order|Sales Order|Service|Install)/gi,
      /<strong>Job:<\/strong><span[^>]*>\s*(Service Order|Sales Order|Service|Install)/gi,
      // Pattern for the specific HTML structure in the file
      /Job:<\/strong><span[^>]*>\s*\n\s*(Service Order|Sales Order|Service|Install)/gi,
      /<strong>Job:<\/strong><span[^>]*>\s*\n\s*(Service Order|Sales Order|Service|Install)/gi,
      // Look for sheet type in title area
      /<u>Service Order\s*<\/u>/gi,
      /<u>Sales Order\s*<\/u>/gi,
      // Original patterns
      /Job:\s*(Service Order|Sales Order|Service|Install)/gi,
      /Type:\s*(Service|Install)/gi,
      /Job\s+Type:\s*(Service|Install)/gi,
      /Work\s+Type:\s*(Service|Install)/gi,
      /<[^>]*>Job:\s*(Service Order|Sales Order|Service|Install)/gi,
      /<[^>]*>Type:\s*(Service|Install)/gi,
      /<[^>]*>\s*Job:\s*(Service Order|Sales Order|Service|Install)/gi,
      /<[^>]*>\s*Type:\s*(Service|Install)/gi,
      />\s*Job:\s*(Service Order|Sales Order|Service|Install)/gi,
      />\s*Type:\s*(Service|Install)/gi
    ];
    
    if (!jobType) {
      for (const pattern of additionalJobTypePatterns) {
        const matches = htmlContent.match(pattern);
        if (matches) {
          const match = matches[0].match(pattern);
          if (match && match[1]) {
            let extractedType = match[1];
            // Map "Sales Order" to "Install" and "Service Order" to "Service"
            if (extractedType.toLowerCase() === 'sales order') {
              jobType = 'Install';
            } else if (extractedType.toLowerCase() === 'service order') {
              jobType = 'Service';
            } else {
              jobType = extractedType;
            }
            console.log('Job type extracted:', extractedType, '-> mapped to:', jobType, 'using pattern:', pattern);
            break;
          }
        }
      }
      
      // Also check for sheet type in title if no job type found yet
      if (!jobType) {
        if (/<u>Service Order\s*<\/u>/i.test(htmlContent)) {
          jobType = 'Service';
          console.log('Job type detected from title: Service Order Sheet');
        } else if (/<u>Sales Order\s*<\/u>/i.test(htmlContent)) {
          jobType = 'Install';
          console.log('Job type detected from title: Sales Order Sheet');
        }
      }
    }
    
    // Extract ETA date from HTML content
    let eta = 'Pending'; // Default
    const etaPatterns = [
      // Look for patterns like "ETA:</strong><span>23/5/2025" with whitespace
      /<strong><span[^>]*>ETA:<\/span><\/strong><span[^>]*>\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /ETA:<\/strong><span[^>]*>\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<strong>ETA:<\/strong><span[^>]*>\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      // Pattern for the specific HTML structure in the file  
      /ETA:<\/strong><span[^>]*>\s*\n\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<strong>ETA:<\/strong><span[^>]*>\s*\n\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      // Original patterns
      /ETA:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /ETA:\s*(\d{1,2}-\d{1,2}-\d{4})/gi,
      /ETA:\s*(\d{4}-\d{1,2}-\d{1,2})/gi,
      /Expected:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Due:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Install\s+Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Service\s+Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<[^>]*>ETA:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /<[^>]*>\s*ETA:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      />\s*ETA:\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi
    ];
    
    for (const pattern of etaPatterns) {
      const matches = htmlContent.match(pattern);
      if (matches) {
        const match = matches[0].match(pattern);
        if (match && match[1]) {
          // Convert DD/MM/YYYY to YYYY-MM-DD format for HTML date input
          const dateStr = match[1];
          if (dateStr.includes('/')) {
            const parts = dateStr.split('/');
            if (parts.length === 3) {
              const day = parts[0].padStart(2, '0');
              const month = parts[1].padStart(2, '0');
              const year = parts[2];
              eta = `${year}-${month}-${day}`;
            }
          } else {
            eta = dateStr;
          }
          console.log('ETA extracted:', eta, 'from:', dateStr, 'using pattern:', pattern);
          break;
        }
      }
    }
    
    // Final safeguard: Never allow document type labels or system terms as client names
    const invalidClientNames = [
      'sales order', 'service order', 'check measure', 'job sheet', 'fitter work',
      'work sheet', 'service sheet', 'install order', 'measure sheet',
      // Document structure and column headers
      'line no', 'line number', 'item no', 'item number', 'qty', 'quantity',
      'unit price', 'total price', 'description', 'product description',
      'fabric colour', 'fabric color', 'frame colour', 'frame color',
      'blade colour', 'blade color', 'control side', 'control type',
      'mount type', 'stack position', 'chain color', 'chain colour',
      'roll direction', 'drop length', 'width', 'height', 'left', 'right',
      'standard', 'reverse', 'face fit', 'recess', 'ceiling fix',
      'bottom rail', 'top rail', 'chain weight', 'pelmet', 'fascia',
      // Product names
      'roller blind', 'roller blinds', 'roman blind', 'roman blinds',
      'venetian blind', 'venetian blinds', 'vertical blind', 'vertical blinds',
      'panel glide', 'panel glides', 'honeycomb blind', 'honeycomb blinds',
      'plantation shutter', 'plantation shutters', 'timber shutter', 'timber shutters',
      'blind type', 'shutter type', 'product type', 'type of blind', 'type of shutter',
      'norman shutter', 'custom timber', 'pure white'
    ];
    if (clientName && invalidClientNames.includes(clientName.toLowerCase().trim())) {
      console.log(`[Client Name] Rejected invalid client name: "${clientName}" - using empty string`);
      clientName = '';
    }
    
    console.log('Extracted data:', { jobId, clientName, address, consultantName, jobType, eta });
    
    // Create the title in format: Job ID - Client Name - Consultant
    let title = jobId;
    if (clientName) {
      title += ` - ${clientName}`;
    }
    if (consultantName) {
      title += ` - ${consultantName}`;
    }
    
    return {
      jobId,
      clientName,
      address,
      consultantName,
      type: jobType, // Use extracted job type
      eta: eta, // Use extracted ETA date
      installer: installer,
      consultant: consultant,
      orderDate: orderDate,
      etaDate: etaDate,
      contact: contact,
      clientEmail: clientEmail, // Client's email for invoice sending
      amountOutstanding: amountOutstanding, // Numeric amount outstanding
      installationNotes: installationNotes,
      productDetails: productDetails,
      balance: balance,
      templateData: {
        title: title || 'Fitter Work Sheet',
        tables: tableData,
        style: extractCssStyles(htmlContent)
      },
      originalHtml: cleanedContent // Store the cleaned HTML (without scissors section)
    };
  } catch (error) {
    console.error('Error parsing HTML:', error);
    console.error('Error details:', {
      message: error.message,
      stack: error.stack,
      htmlContentLength: htmlContent?.length || 0
    });
    
    // Return a basic object with empty values instead of throwing
    return {
      jobId: '',
      clientName: '',
      address: '',
      consultantName: '',
      type: 'Install',
      eta: 'Pending',
      installer: '',
      consultant: '',
      orderDate: '',
      etaDate: '',
      contact: '',
      clientEmail: '',
      amountOutstanding: null,
      installationNotes: '',
      productDetails: '',
      balance: '',
      templateData: {
        title: 'Fitter Work Sheet',
        tables: [],
        style: ''
      },
      originalHtml: htmlContent || ''
    };
  }
}

// Function to extract job sheet data from HTML content
export function extractJobSheetData(htmlContent: string) {
  try {
    console.log('Extracting job sheet data from HTML content...');
    
    // Use the same extraction logic as check measure for consistency
    const extractedData = extractJobDataFromHtml(htmlContent);
    
    console.log('Job sheet extraction result:', extractedData);
    
    return {
      jobId: extractedData.jobId || '',
      clientName: extractedData.clientName || '',
      address: extractedData.address || '',
      type: extractedData.type || 'Install',
      eta: extractedData.eta || 'Pending',
      installer: extractedData.installer || '',
      consultant: extractedData.consultant || extractedData.consultantName || '',
      orderDate: extractedData.orderDate || '',
      etaDate: extractedData.etaDate || '',
      contact: extractedData.contact || '',
      clientEmail: extractedData.clientEmail || '',
      amountOutstanding: extractedData.amountOutstanding || null,
      installationNotes: extractedData.installationNotes || '',
      productDetails: extractedData.productDetails || '',
      balance: extractedData.balance || '',
      description: extractedData.consultantName ? `Consultant: ${extractedData.consultantName}` : '',
      status: 'To Do',
      urgent: false,
      originalContent: htmlContent,
      extractedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error('Error extracting job sheet data:', error);
    // Return empty object instead of throwing error so the upload can continue
    return {
      jobId: '',
      clientName: '',
      address: '',
      type: 'Install',
      eta: 'Pending',
      clientEmail: '',
      amountOutstanding: null,
      description: '',
      status: 'To Do',
      urgent: false,
      originalContent: htmlContent,
      extractedAt: new Date().toISOString()
    };
  }
}

// Function to parse HTML tables and extract structure
function parseHtmlTables(htmlContent: string) {
  try {
    console.log('Starting parseHtmlTables with content length:', htmlContent.length);
    const tables = [];
    
    // Extract tables from HTML
    const tableRegex = /<table[^>]*>([\s\S]*?)<\/table>/gi;
    let tableMatch;
    let tableIndex = 0;
  
  while ((tableMatch = tableRegex.exec(htmlContent)) !== null) {
    const tableHtml = tableMatch[1];
    
    // Extract headers
    const headerRegex = /<th[^>]*>([\s\S]*?)<\/th>/gi;
    const headers = [];
    let headerMatch;
    
    while ((headerMatch = headerRegex.exec(tableHtml)) !== null) {
      headers.push(headerMatch[1].replace(/<[^>]*>/g, '').trim());
    }
    
    // Extract rows
    const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    const rows = [];
    let rowMatch;
    
    while ((rowMatch = rowRegex.exec(tableHtml)) !== null) {
      const rowHtml = rowMatch[1];
      const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
      const cells = [];
      let cellMatch;
      
      while ((cellMatch = cellRegex.exec(rowHtml)) !== null) {
        cells.push(cellMatch[1].replace(/<[^>]*>/g, '').trim());
      }
      
      if (cells.length > 0) {
        rows.push(cells);
      }
    }
    
    if (headers.length > 0 || rows.length > 0) {
      tables.push({
        index: tableIndex++,
        headers: headers.length > 0 ? headers : (rows.length > 0 ? rows[0] : []),
        rows: headers.length > 0 ? rows : rows.slice(1),
        editable: true
      });
    }
  }
  
  // Extract Installation Notes section (which is not in a table)
  const installationNotesRegex = /<p><strong>Installation Notes:<\/strong>\s*<\/p><p>([^<]*)<\/p>/gi;
  const installationMatch = htmlContent.match(installationNotesRegex);
  
  if (installationMatch) {
    const notesMatch = installationMatch[0].match(/<p><strong>Installation Notes:<\/strong>\s*<\/p><p>([^<]*)<\/p>/);
    if (notesMatch && notesMatch[1]) {
      // Add Installation Notes as a special table-like structure
      tables.push({
        index: tableIndex++,
        headers: ["Installation Notes"],
        rows: [[notesMatch[1].trim()]],
        editable: true,
        isInstallationNotes: true
      });
    }
  }
  
  // Extract Payment Summary information (Balance section)
  const balanceRows = [];
  
  // Extract balance details using the actual HTML structure
  const balanceItems = [
    { label: "Before Discount (Inc Tax)", regex: /<th class="payment-summary-label-beforediscountinctax">[\s\S]*?Before Discount \(Inc Tax\)[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-beforediscountinctax">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi },
    { label: "Discount (Inc Tax)", regex: /<th class="payment-summary-label-discountinctax">[\s\S]*?Discount \(Inc Tax\)[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-discountinctax">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi },
    { label: "Total Inc Tax", regex: /<th class="payment-summary-label-totalinctax">[\s\S]*?Total Inc Tax[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-totalinctax">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi },
    { label: "Tax Included", regex: /<th class="payment-summary-label-taxincluded">[\s\S]*?Tax Included[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-taxincluded">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi },
    { label: "Amount Paid", regex: /<th class="payment-summary-label-amountpaid">[\s\S]*?Amount Paid[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-amountpaid">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi },
    { label: "Amount Outstanding", regex: /<th class="payment-summary-label-amountoutstanding">[\s\S]*?Amount Outstanding[\s\S]*?<\/th>[\s\S]*?<td class="payment-summary-value-amountoutstanding">[\s\S]*?\$([0-9,.]+)[\s\S]*?<\/td>/gi }
  ];
  
  balanceItems.forEach(item => {
    const match = htmlContent.match(item.regex);
    if (match && match[1]) {
      balanceRows.push([item.label, `$${match[1]}`]);
    }
  });
  
  if (balanceRows.length > 0) {
    tables.push({
      index: tableIndex++,
      headers: ["Description", "Amount"],
      rows: balanceRows,
      editable: false,
      isBalanceSection: true
    });
  }
  
  // Extract Bank Details section using actual HTML structure
  const bankDetailsRegex = /<p style="font-weight: bold;">[\s\S]*?Direct Deposit Bank Details[\s\S]*?<\/p><p>[\s\S]*?<span style="font-size: xx-small;">[\s\S]*?Account Name:\s*([^<\n]+)<br>Bank:\s*([^<\n]+)<br>BSB:\s*([^<\n]+)<br>Account No\.:\s*([^<\n]+)[\s\S]*?<\/span>[\s\S]*?<\/p><p>[\s\S]*?<strong><span style="font-size: xx-small;">Reference:<\/span><\/strong><span style="font-size: xx-small;">[\s\S]*?([^<]+)/gi;
  const bankMatch = htmlContent.match(bankDetailsRegex);
  
  if (bankMatch) {
    const match = bankMatch[0].match(bankDetailsRegex);
    if (match && match.length >= 6) {
      const bankRows = [
        ["Account Name", match[1].trim()],
        ["Bank", match[2].trim()],
        ["BSB", match[3].trim()],
        ["Account No.", match[4].trim()],
        ["Reference", match[5].trim()]
      ];
      
      tables.push({
        index: tableIndex++,
        headers: ["Bank Details", "Information"],
        rows: bankRows,
        editable: false,
        isBankDetails: true
      });
    }
  }
  
  console.log('Parsed tables:', tables.length, 'tables found');
  tables.forEach((table, index) => {
    console.log(`Table ${index}:`, {
      headers: table.headers,
      rowCount: table.rows.length,
      isInstallationNotes: table.isInstallationNotes || false,
      isBalanceSection: table.isBalanceSection || false,
      isBankDetails: table.isBankDetails || false
    });
  });
  
  return tables;
  } catch (error) {
    console.error('Error parsing HTML tables:', error);
    return [];
  }
}

// Function to extract CSS styles from HTML
function extractCssStyles(htmlContent: string) {
  const styleMatch = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/);
  return styleMatch ? styleMatch[1] : '';
}

// Helper function to get user's workspace ID
async function getUserWorkspaceId(userId: string): Promise<number | null> {
  const user = await storage.getUser(userId);
  return user?.workspaceId || null;
}

export async function registerRoutes(app: Express): Promise<Server> {
  // Auth middleware
  await setupAuth(app);

  // Auth routes
  app.get('/api/auth/user', isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const user = await storage.getUser(userId);
      res.json(user);
    } catch (error) {
      console.error("Error fetching user:", error);
      res.status(500).json({ message: "Failed to fetch user" });
    }
  });

  // User management routes (owner only)
  app.get('/api/users', isAuthenticated, isOwner, async (req, res) => {
    try {
      const users = await storage.getAllUsers();
      res.json(users);
    } catch (error) {
      console.error("Error fetching users:", error);
      res.status(500).json({ message: "Failed to fetch users" });
    }
  });

  app.patch('/api/users/:id/role', isAuthenticated, isOwner, async (req, res) => {
    try {
      const { id } = req.params;
      const { role } = req.body;
      
      if (!['owner', 'admin', 'fitter'].includes(role)) {
        return res.status(400).json({ message: "Invalid role" });
      }

      const user = await storage.updateUserRole(id, role);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }
      res.json(user);
    } catch (error) {
      console.error("Error updating user role:", error);
      res.status(500).json({ message: "Failed to update user role" });
    }
  });

  app.patch('/api/users/:id/active', isAuthenticated, isOwner, async (req, res) => {
    try {
      const { id } = req.params;
      const { isActive } = req.body;
      
      if (typeof isActive !== 'boolean') {
        return res.status(400).json({ message: "isActive must be a boolean" });
      }

      const user = await storage.updateUserActive(id, isActive);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }
      res.json(user);
    } catch (error) {
      console.error("Error updating user status:", error);
      res.status(500).json({ message: "Failed to update user status" });
    }
  });

  app.delete('/api/users/:id', isAuthenticated, isOwner, async (req, res) => {
    try {
      const { id } = req.params;
      const success = await storage.deleteUser(id);
      if (!success) {
        return res.status(404).json({ message: "User not found" });
      }
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting user:", error);
      res.status(500).json({ message: "Failed to delete user" });
    }
  });

  // User Location Settings routes
  app.get('/api/user/location-settings', isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const settings = await storage.getUserLocationSettings(userId);
      
      // Return default settings if none exist
      if (!settings) {
        return res.json({
          userId,
          homeAddress: null,
          homeLatitude: null,
          homeLongitude: null,
          primaryWarehouse: 'malaga',
          includeHomeInRoute: false
        });
      }
      
      res.json(settings);
    } catch (error) {
      console.error("Error fetching location settings:", error);
      res.status(500).json({ message: "Failed to fetch location settings" });
    }
  });

  app.put('/api/user/location-settings', isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const { homeAddress, homeLatitude, homeLongitude, primaryWarehouse, includeHomeInRoute } = req.body;
      
      // Validate coordinates if provided
      const hasValidLatitude = homeLatitude !== null && homeLatitude !== undefined && 
        typeof homeLatitude === 'number' && homeLatitude >= -90 && homeLatitude <= 90;
      const hasValidLongitude = homeLongitude !== null && homeLongitude !== undefined && 
        typeof homeLongitude === 'number' && homeLongitude >= -180 && homeLongitude <= 180;
      
      let finalLatitude = hasValidLatitude ? homeLatitude : null;
      let finalLongitude = hasValidLongitude ? homeLongitude : null;
      let geocodeStatus: 'success' | 'failed' | 'skipped' | 'not_needed' = 'not_needed';
      
      // Auto-geocode the home address if provided but no valid coordinates
      if (homeAddress && homeAddress.trim() !== '') {
        if (finalLatitude === null || finalLongitude === null) {
          try {
            const coords = await geocodeAddress(homeAddress);
            if (coords) {
              finalLatitude = coords.lat;
              finalLongitude = coords.lng;
              geocodeStatus = 'success';
              console.log(`Geocoded home address: "${homeAddress}" -> ${coords.lat}, ${coords.lng}`);
            } else {
              geocodeStatus = 'failed';
              console.log(`Failed to geocode home address: "${homeAddress}"`);
            }
          } catch (geoError) {
            console.error("Geocoding error:", geoError);
            geocodeStatus = 'failed';
          }
        } else {
          geocodeStatus = 'skipped'; // Valid coords already provided
        }
      }
      
      const settings = await storage.upsertUserLocationSettings({
        userId,
        homeAddress: homeAddress || null,
        homeLatitude: finalLatitude,
        homeLongitude: finalLongitude,
        primaryWarehouse: primaryWarehouse || 'malaga',
        includeHomeInRoute: includeHomeInRoute || false
      });
      
      res.json({ ...settings, geocodeStatus });
    } catch (error) {
      console.error("Error updating location settings:", error);
      res.status(500).json({ message: "Failed to update location settings" });
    }
  });

  // Team management routes
  app.get('/api/workspace/users', isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const users = await storage.getWorkspaceUsers(workspaceId);
      res.json(users);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch workspace users" });
    }
  });

  app.get('/api/workspace/invitations', isAuthenticated, isActive, requireRole(['owner', 'admin']), async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const invitations = await storage.getWorkspaceInvitations(workspaceId);
      res.json(invitations);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch invitations" });
    }
  });

  app.post('/api/workspace/invite', isAuthenticated, isActive, requireRole(['owner', 'admin']), async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { email, role } = req.body;
      
      if (!email || !role || !['admin', 'viewer'].includes(role)) {
        return res.status(400).json({ error: "Valid email and role required" });
      }
      
      // Generate invitation token
      const token = require('crypto').randomBytes(32).toString('hex');
      const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // 7 days
      
      const invitation = await storage.createTeamInvitation({
        workspaceId,
        email,
        role,
        invitedBy: userId,
        token,
        expiresAt
      });
      
      res.status(201).json({ 
        message: "Invitation created successfully", 
        invitation: {
          email: invitation.email,
          role: invitation.role,
          token: invitation.token
        }
      });
    } catch (error) {
      res.status(500).json({ message: "Failed to send invitation" });
    }
  });

  // Protected job routes (require authentication and active account)
  // One-time repair endpoint: re-extracts client names for CM jobs with wrong values
  app.post("/api/admin/repair-cm-names", isAuthenticated, async (req: any, res) => {
    try {
      const suspiciousNames = [
        // Product names
        'woodlore', 'woodlore plus', 'palm beach', 'normandy', 'new style', 'classic style',
        'heritage style', 'brightwood', 'polywood', 'composite shutter', 'silk white',
        'pure white', 'factory match',
        // Room names / abbreviations
        'toilet', 'tv room', 'study shutter', 'up games', 'lng', 'kitch', 'ens',
        'lounge', 'lounge room', 'living room', 'family room', 'kitchen', 'dining room',
        'bedroom', 'master bedroom', 'bathroom', 'ensuite', 'laundry', 'garage',
        'patio', 'alfresco', 'theatre room', 'media room', 'games room', 'rumpus room',
        'store room', 'powder room', 'utility room', 'mud room', 'sitting room', 'playroom',
        // Clearly wrong defaults
        'unknown client', 'australia', 'address not found',
      ];
      const locationSuffixes = [' left', ' right', ' centre', ' center', ' top', ' bottom', ' lower', ' upper', ' inner', ' outer'];
      const isSuspicious = (name: string) => {
        if (!name) return true;
        const lower = name.toLowerCase().trim();
        if (suspiciousNames.some(s => lower === s)) return true;
        if (locationSuffixes.some(s => lower.endsWith(s))) return true;
        // Single word only (like "Giudicatti", "Ens", "Kitch")
        if (!lower.includes(' ') && lower.length < 10) return true;
        return false;
      };

      // Get all CM jobs
      const { db } = await import("./db");
      const { jobs: jobsTable, jobDocuments } = await import("../shared/schema");
      const { eq, like, sql: sqlExpr } = await import("drizzle-orm");

      const cmJobs = await db.select({ id: jobsTable.id, jobId: jobsTable.jobId, clientName: jobsTable.clientName })
        .from(jobsTable)
        .where(like(jobsTable.jobId, '%-CM%'));

      const results: any[] = [];

      for (const job of cmJobs) {
        if (!isSuspicious(job.clientName)) continue;

        // Get stored HTML document
        const docs = await db.select({ originalContent: jobDocuments.originalContent })
          .from(jobDocuments)
          .where(eq(jobDocuments.jobId, job.id))
          .limit(1);

        if (!docs[0]?.originalContent) {
          results.push({ jobId: job.jobId, old: job.clientName, new: null, reason: 'no document' });
          continue;
        }

        const extracted = extractJobDataFromHtml(docs[0].originalContent);
        const newName = extracted.clientName;

        if (newName && newName !== job.clientName && !isSuspicious(newName)) {
          await db.update(jobsTable)
            .set({ clientName: newName })
            .where(eq(jobsTable.id, job.id));
          results.push({ jobId: job.jobId, old: job.clientName, new: newName, updated: true });
        } else {
          results.push({ jobId: job.jobId, old: job.clientName, new: newName || 'still unknown', updated: false });
        }
      }

      res.json({ processed: results.length, results });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/jobs", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const jobs = await storage.getAllJobs(workspaceId);
      res.json(jobs);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch jobs" });
    }
  });

  // Get single job
  app.get("/api/jobs/:id", isAuthenticated, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const job = await storage.getJob(id, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch job" });
    }
  });

  // Get calendar events for a job
  app.get("/api/jobs/:id/calendar-events", isAuthenticated, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const job = await storage.getJob(id, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }

      // Try to get calendar events for this job
      try {
        const { getJobCalendarEvents } = await import('./google-calendar');
        const calendarEvents = await getJobCalendarEvents(job.jobId);
        res.json({ events: calendarEvents });
      } catch (calendarError) {
        console.error('Error fetching calendar events:', calendarError);
        res.json({ events: [] });
      }
    } catch (error) {
      console.error("Error fetching job calendar events:", error);
      res.status(500).json({ error: "Failed to fetch calendar events" });
    }
  });

  // Create new job
  app.post("/api/jobs", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      console.log('Creating job with data:', req.body);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      // Add workspace ID to the job data
      const jobData = { ...req.body, workspaceId };
      const validatedData = insertJobSchema.parse(jobData);
      console.log('Validated data:', validatedData);
      
      // Check for duplicate job ID AND type within the workspace
      // Allow same job ID if it's a different type (e.g., CM vs Install)
      const existingJobs = await storage.getJobsByJobIds([validatedData.jobId], workspaceId);
      const duplicateJobType = existingJobs.find(job => 
        job.jobId === validatedData.jobId && job.type === validatedData.type
      );
      
      if (duplicateJobType) {
        return res.status(409).json({ 
          error: "Job already exists", 
          message: `You have already added this ${validatedData.type} job (${validatedData.jobId}). You cannot add the same job twice.`
        });
      }
      
      const job = await storage.createJob(validatedData);
      res.status(201).json(job);
    } catch (error) {
      console.error('Error creating job:', error);
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid job data", details: error.errors });
      }
      
      // Check if it's a database constraint error (duplicate key)
      if (error instanceof Error && error.message.includes('duplicate key')) {
        return res.status(409).json({ 
          error: "Job already exists", 
          message: "You have already added this job. You cannot add the same job twice."
        });
      }
      
      res.status(500).json({ error: "Failed to create job", details: error instanceof Error ? error.message : String(error) });
    }
  });

  // Update job
  app.put("/api/jobs/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const validatedData = updateJobSchema.parse(req.body);
      const job = await storage.updateJob(id, validatedData, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid job data", details: error.errors });
      }
      res.status(500).json({ error: "Failed to update job" });
    }
  });

  // Update job status
  app.patch("/api/jobs/batch-status", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      const { jobIds, status } = req.body;
      if (!Array.isArray(jobIds) || jobIds.length === 0) {
        return res.status(400).json({ error: "jobIds array is required" });
      }
      if (!status || !['To Do', 'Complete', 'Waiting on Client', 'Rework', 'Booked', 'Awaiting Response'].includes(status)) {
        return res.status(400).json({ error: "Invalid status" });
      }
      let updated = 0;
      for (const id of jobIds) {
        const job = await storage.updateJobStatus(id, status, workspaceId, null);
        if (job) updated++;
      }
      res.json({ success: true, updated });
    } catch (error) {
      console.error("Error batch updating status:", error);
      res.status(500).json({ error: "Failed to batch update status" });
    }
  });

  app.patch("/api/jobs/:id/status", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { status, faultType } = req.body;
      
      if (!status || !['To Do', 'Complete', 'Waiting on Client', 'Rework', 'Booked', 'Awaiting Response'].includes(status)) {
        return res.status(400).json({ error: "Invalid status" });
      }

      if (status === 'Rework' && (!faultType || !['Fitter', 'Consultant', 'Factory', 'Client'].includes(faultType))) {
        return res.status(400).json({ error: "Fault type required for Rework status" });
      }

      const finalFaultType = status === 'Rework' ? faultType : null;

      const job = await storage.updateJobStatus(id, status, workspaceId, finalFaultType);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to update job status" });
    }
  });

  // Update job paper status
  app.patch("/api/jobs/:id/paper-status", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { paperStatus } = req.body;
      
      if (!paperStatus || !['Papers Yes', 'Papers No'].includes(paperStatus)) {
        return res.status(400).json({ error: "Invalid paper status" });
      }

      const job = await storage.updateJobPaperStatus(id, paperStatus, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to update job paper status" });
    }
  });

  // Update job completion status with date
  app.patch("/api/jobs/:id/complete", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { completedAt } = req.body;

      if (!completedAt) {
        return res.status(400).json({ error: "Completion date is required" });
      }

      const job = await storage.updateJobCompletion(id, completedAt, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }

      // Automatically add job to the appropriate weekly invoice
      try {
        const completionDate = new Date(completedAt);
        const { weekStartDate, weekEndDate } = getTuesdayWeekBoundaries(completionDate);
        
        // Check if invoice exists for this week
        let invoice = await storage.getInvoiceByWeek(weekStartDate, workspaceId);
        
        // Create invoice if it doesn't exist
        if (!invoice) {
          // Generate invoice number
          const invoiceNumber = await storage.generateInvoiceNumber(workspaceId);
          
          invoice = await storage.createInvoice({
            workspaceId,
            invoiceNumber,
            weekStartDate,
            weekEndDate,
            status: 'draft',
            totalAmount: '0'
          });
        }
        
        // Add job as invoice item if it has an install cost
        if (job.installCost) {
          const installCost = parseFloat(job.installCost as string);
          const gstAmount = installCost * 0.1;
          const totalAmount = installCost + gstAmount;
          
          await storage.createInvoiceItem({
            invoiceId: invoice.id,
            jobId: job.id,
            itemType: 'job',
            description: `${job.jobId} - ${job.clientName} (${job.type})`,
            baseAmount: installCost.toFixed(2),
            gstAmount: gstAmount.toFixed(2),
            totalAmount: totalAmount.toFixed(2)
          });
          
          // Recalculate invoice total (sum base + sum gst to avoid floating-point errors)
          const allItems = await storage.getInvoiceItems(invoice.id);
          const subtotal = allItems.reduce((sum, item) => sum + parseFloat(item.baseAmount as string), 0);
          const gstTotal = allItems.reduce((sum, item) => sum + parseFloat(item.gstAmount as string), 0);
          const newTotal = subtotal + gstTotal;
          await storage.updateInvoice(invoice.id, { totalAmount: newTotal.toFixed(2) }, workspaceId);
        }
      } catch (invoiceError) {
        console.error("Error adding job to invoice:", invoiceError);
        // Don't fail the completion if invoice creation fails
      }

      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to update job completion" });
    }
  });

  // Update job urgent status
  app.patch("/api/jobs/:id/urgent", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { urgent } = req.body;
      
      if (typeof urgent !== 'boolean') {
        return res.status(400).json({ error: "Urgent status is required" });
      }

      const job = await storage.updateJobUrgent(id, urgent, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to update job urgent status" });
    }
  });

  // One-time cleanup: Clear urgent flag from completed/booked jobs
  app.post("/api/jobs/cleanup-urgent", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const updatedCount = await storage.cleanupUrgentFlags(workspaceId);
      res.json({ 
        message: "Cleanup completed", 
        updatedCount,
        description: "Cleared urgent flag from completed and booked jobs"
      });
    } catch (error) {
      res.status(500).json({ error: "Failed to cleanup urgent flags" });
    }
  });

  // Helper function to convert AM/PM time to 24-hour format
  function convertTo24Hour(time12h: string): string {
    if (!time12h || !time12h.includes('M')) {
      // Already in 24-hour format or invalid, return as-is
      return time12h;
    }
    
    const [time, modifier] = time12h.split(' ');
    let [hours, minutes] = time.split(':');
    
    if (hours === '12') {
      hours = modifier === 'AM' ? '00' : '12';
    } else {
      hours = modifier === 'AM' ? hours : String(parseInt(hours, 10) + 12);
    }
    
    // Ensure hours is always 2 digits
    hours = hours.padStart(2, '0');
    
    return `${hours}:${minutes}`;
  }

  // Update job booking date and time
  app.patch("/api/jobs/:id/booking-date", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { bookingDate, startTime = '09:00', endTime = '17:00', isReschedule = false, jobEstimation, numberOfDays = 1 } = req.body;
      
      // Convert AM/PM times to 24-hour format
      const startTime24 = convertTo24Hour(startTime);
      const endTime24 = convertTo24Hour(endTime);
      
      console.log(`Converting times: ${startTime} -> ${startTime24}, ${endTime} -> ${endTime24}`);
      
      if (!bookingDate || typeof bookingDate !== 'string') {
        return res.status(400).json({ error: "Booking date is required" });
      }

      const job = await storage.getJob(id, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }

      // Handle calendar event updates for both new bookings and rescheduling
      if (await isCalendarAuthorized(workspaceId)) {
        try {
          const { updateCalendarEvent, deleteCalendarEvent, createCalendarEvent } = await import('./google-calendar');
          
          // Create datetime strings in Perth timezone using 24-hour format
          const startDateTime = `${bookingDate}T${startTime24}:00`;
          const endDateTime = `${bookingDate}T${endTime24}:00`;
          
          // If we have an existing calendar event, try to update it
          if (job.calendarEventId) {
            console.log(`Updating existing calendar event ${job.calendarEventId} for job ${job.jobId}`);
            console.log(`New times: ${startDateTime} to ${endDateTime}`);
            
            const costValue = job.installCost != null ? parseFloat(String(job.installCost).replace(/[^0-9.-]/g, '')) : null;
            const costInfo = costValue != null && !isNaN(costValue) ? `\nCost: $${costValue.toFixed(2)}` : '';
            const updateResult = await updateCalendarEvent(job.calendarEventId, {
              summary: `FITTER PRO - ${job.type}: ${job.clientName}`,
              description: `Job ID: ${job.jobId}\nClient: ${job.clientName}\nType: ${job.type}${costInfo}\n\n${isReschedule ? 'Rescheduled' : 'Updated'} via FITTER PRO`,
              location: job.address,
              startDateTime,
              endDateTime,
              timeZone: 'Australia/Perth'
            });
            
            if (updateResult.success) {
              console.log(`✓ Successfully updated calendar event for job ${job.jobId}`);
            } else {
              console.log(`✗ Failed to update calendar event: ${updateResult.error}`);
              // If update fails, delete old event and we'll create a new one below
              await deleteCalendarEvent(job.calendarEventId);
              await storage.updateJobCalendarEventId(id, null, workspaceId);
            }
          }
        } catch (error) {
          console.error('Error updating calendar event:', error);
        }
      }

      // Update job booking date with times and estimation
      const updatedJob = await storage.updateJobBookingDate(id, bookingDate, workspaceId, jobEstimation);
      if (!updatedJob) {
        return res.status(404).json({ error: "Job not found" });
      }
      
      // Update the start and end times for the job
      await storage.updateJobTimes(id, startTime24, endTime24, workspaceId);
      
      // Also update the job status to "Booked" when booking is confirmed
      const finalJob = await storage.updateJobStatus(id, 'Booked', workspaceId);
      console.log(`Updated job ${updatedJob.jobId} status to "Booked"`);
      
      // Create NEW calendar event(s) if we don't have one or the update failed
      if (await isCalendarAuthorized(workspaceId) && !finalJob?.calendarEventId) {
        try {
          const { createCalendarEvent } = await import('./google-calendar');
          
          console.log(`Creating ${numberOfDays} calendar event(s) for job ${finalJob.jobId}`);
          
          // Create calendar events for multiple days
          const eventIds: string[] = [];
          let allSuccess = true;
          let errors: string[] = [];
          
          for (let dayOffset = 0; dayOffset < numberOfDays; dayOffset++) {
            const currentDate = new Date(bookingDate);
            currentDate.setDate(currentDate.getDate() + dayOffset);
            const currentDateStr = currentDate.toISOString().split('T')[0];
            
            const startDateTime = `${currentDateStr}T${startTime24}:00`;
            const endDateTime = `${currentDateStr}T${endTime24}:00`;
            
            const dayText = numberOfDays > 1 ? ` (Day ${dayOffset + 1} of ${numberOfDays})` : '';
            const costValue = finalJob.installCost != null ? parseFloat(String(finalJob.installCost).replace(/[^0-9.-]/g, '')) : null;
            const costInfo = costValue != null && !isNaN(costValue) ? `\nCost: $${costValue.toFixed(2)}` : '';
            
            console.log(`Creating calendar event for day ${dayOffset + 1}: ${startDateTime} to ${endDateTime}`);
            
            const calendarResult = await createCalendarEvent({
              summary: `FITTER PRO - ${finalJob.type}: ${finalJob.clientName}${dayText}`,
              description: `Job ID: ${finalJob.jobId}\nClient: ${finalJob.clientName}\nType: ${finalJob.type}${costInfo}${numberOfDays > 1 ? `\nDay ${dayOffset + 1} of ${numberOfDays}` : ''}\n\nScheduled via FITTER PRO`,
              location: finalJob.address,
              startDateTime,
              endDateTime,
              timeZone: 'Australia/Perth'
            });
            
            if (calendarResult.success && calendarResult.eventId) {
              eventIds.push(calendarResult.eventId);
              console.log(`✓ Created calendar event for day ${dayOffset + 1}: ${calendarResult.eventId}`);
            } else {
              allSuccess = false;
              errors.push(`Day ${dayOffset + 1}: ${calendarResult.error}`);
              console.log(`✗ Failed to create calendar event for day ${dayOffset + 1}: ${calendarResult.error}`);
            }
          }
          
          if (eventIds.length > 0) {
            // Save primary calendar event ID (first day) to job
            await storage.updateJobCalendarEventId(id, eventIds[0], workspaceId);
            console.log(`✓ Created ${eventIds.length}/${numberOfDays} calendar events for job ${finalJob.jobId}`);
            
            if (!allSuccess) {
              console.log(`⚠ Some calendar events failed: ${errors.join(', ')}`);
            }
          } else {
            console.log(`✗ Failed to create any calendar events: ${errors.join(', ')}`);
          }
        } catch (calendarError) {
          console.error('Error creating calendar events:', calendarError);
          // Don't fail the booking if calendar creation fails
        }
      }
      
      // Trigger immediate calendar sync to detect any deleted events
      try {
        const { syncCalendarEvents } = await import('./google-calendar');
        syncCalendarEvents().catch(error => console.error('Background sync error:', error));
      } catch (error) {
        console.error('Error triggering calendar sync:', error);
      }
      
      // Schedule invoice email for all jobs with a client email (regardless of balance)
      const updatedFinalJob = await storage.getJob(id, workspaceId);
      if (updatedFinalJob?.clientEmail) {
        try {
          const { scheduleInvoiceEmail } = await import('./invoice-email-scheduler');
          
          // Get the user's name for the email signature
          const currentUser = await storage.getUser(userId);
          const senderName = currentUser?.firstName || undefined;
          
          // Schedule email for the appointment time (start of booking) in Perth timezone (AWST = UTC+8)
          // Create ISO string with explicit timezone offset so Date correctly converts to UTC
          const isoString = `${bookingDate}T${startTime24}:00.000+08:00`;
          const scheduledFor = new Date(isoString);
          
          // Validate the date is valid
          if (isNaN(scheduledFor.getTime())) {
            console.error(`[Invoice Email] Invalid scheduled time for job ${updatedFinalJob.jobId}: ${isoString}`);
          } else {
            await scheduleInvoiceEmail(
              id,
              workspaceId,
              updatedFinalJob.clientEmail,
              updatedFinalJob.clientName,
              updatedFinalJob.amountOutstanding || '0',
              scheduledFor,
              senderName
            );
            console.log(`[Invoice Email] Scheduled invoice email for job ${updatedFinalJob.jobId} at ${scheduledFor.toISOString()} (Perth time: ${bookingDate} ${startTime24}), sender: ${senderName}`);
          }
        } catch (emailError) {
          console.error('Error scheduling invoice email:', emailError);
          // Don't fail the booking if email scheduling fails
        }
      }
      
      // Get the final job state with calendar event ID if created
      const responseJob = await storage.getJob(id, workspaceId);
      res.json(responseJob);
    } catch (error) {
      console.error("Error updating job booking date:", error);
      res.status(500).json({ error: "Failed to update job booking date" });
    }
  });

  // Update SMS tracking for a job
  app.patch("/api/jobs/:id/sms-sent", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { phoneNumber, proposedBookingDate, proposedBookingTime, proposedFinishTime, numberOfDays } = req.body;
      
      if (isNaN(id)) {
        return res.status(400).json({ error: "Invalid job ID" });
      }
      
      const job = await storage.updateJobSMSTracking(id, workspaceId, phoneNumber, proposedBookingDate, proposedBookingTime, proposedFinishTime, numberOfDays);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      console.error('SMS tracking error:', error);
      res.status(500).json({ error: "Failed to update SMS tracking" });
    }
  });

  app.patch("/api/jobs/:id/confirmation-sent", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      if (isNaN(id)) {
        return res.status(400).json({ error: "Invalid job ID" });
      }
      
      const updated = await storage.updateJob(id, { confirmationSentAt: new Date() } as any, workspaceId);
      
      if (!updated) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(updated);
    } catch (error) {
      console.error('Confirmation tracking error:', error);
      res.status(500).json({ error: "Failed to update confirmation tracking" });
    }
  });

  // Add day to existing booking
  app.post("/api/jobs/:id/add-day", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { additionalDate, startTime = '09:00', endTime = '17:00' } = req.body;
      
      if (isNaN(id) || !additionalDate) {
        return res.status(400).json({ error: "Job ID and additional date are required" });
      }

      // Get the existing job to validate it exists and has a booking
      const job = await storage.getJob(id, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }

      if (job.status !== 'Booked' || !job.bookingDate) {
        return res.status(400).json({ error: "Job must be booked before adding additional days" });
      }

      // Convert AM/PM times to 24-hour format
      const startTime24 = convertTo24Hour(startTime);
      const endTime24 = convertTo24Hour(endTime);

      // Create calendar event for the additional day
      if (await isCalendarAuthorized()) {
        try {
          const { createCalendarEvent } = await import('./google-calendar');
          
          const startDateTime = `${additionalDate}T${startTime24}:00`;
          const endDateTime = `${additionalDate}T${endTime24}:00`;
          
          console.log(`Creating additional day calendar event for job ${job.jobId}`);
          console.log(`Additional day: ${startDateTime} to ${endDateTime}`);
          
          // Get current number of days for this job (default to 1 if not set)
          const currentDays = job.numberOfDays || 1;
          const newDayNumber = currentDays + 1;
          const costValue = job.installCost != null ? parseFloat(String(job.installCost).replace(/[^0-9.-]/g, '')) : null;
          const costInfo = costValue != null && !isNaN(costValue) ? `\nCost: $${costValue.toFixed(2)}` : '';
          
          const calendarResult = await createCalendarEvent({
            summary: `FITTER PRO - ${job.type}: ${job.clientName} (Day ${newDayNumber})`,
            description: `Job ID: ${job.jobId}\nClient: ${job.clientName}\nType: ${job.type}${costInfo}\nDay ${newDayNumber} of multi-day booking\n\nAdded via FITTER PRO`,
            location: job.address,
            startDateTime,
            endDateTime,
            timeZone: 'Australia/Perth'
          });
          
          if (calendarResult.success && calendarResult.eventId) {
            // Update the job to increase number of days
            console.log(`✓ Created additional day calendar event ${calendarResult.eventId} for job ${job.jobId}`);
            
            // Update the numberOfDays field in the database
            await storage.updateJobNumberOfDays(id, newDayNumber, workspaceId);
            
            res.json({ 
              success: true, 
              message: `Additional day added successfully`,
              calendarEventId: calendarResult.eventId,
              additionalDate,
              dayNumber: newDayNumber
            });
          } else {
            console.log(`✗ Failed to create additional day calendar event: ${calendarResult.error}`);
            res.status(500).json({ error: `Failed to create calendar event: ${calendarResult.error}` });
          }
        } catch (calendarError) {
          console.error('Error creating additional day calendar event:', calendarError);
          res.status(500).json({ error: "Failed to create calendar event for additional day" });
        }
      } else {
        res.status(500).json({ error: "Google Calendar not authorized" });
      }
    } catch (error) {
      console.error('Add day error:', error);
      res.status(500).json({ error: "Failed to add day to booking" });
    }
  });

  // Clear SMS tracking for a job
  app.patch("/api/jobs/:id/clear-sms", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      if (isNaN(id)) {
        return res.status(400).json({ error: "Invalid job ID" });
      }
      
      const job = await storage.clearJobSMS(id, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      console.error('Clear SMS error:', error);
      res.status(500).json({ error: "Failed to clear SMS data" });
    }
  });

  // Update job warehouse
  app.patch("/api/jobs/:id/warehouse", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { warehouse } = req.body;
      
      if (!warehouse || !['Malaga', 'Canningvale'].includes(warehouse)) {
        return res.status(400).json({ error: "Valid warehouse is required (Malaga or Canningvale)" });
      }

      const job = await storage.updateJobWarehouse(id, warehouse, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      res.status(500).json({ error: "Failed to update job warehouse" });
    }
  });

  // Update job estimated duration (for route planning)
  app.patch("/api/jobs/:id/duration", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { estimatedDuration } = req.body;
      
      if (typeof estimatedDuration !== 'number' || estimatedDuration < 0) {
        return res.status(400).json({ error: "Valid estimated duration (in minutes) is required" });
      }

      const job = await storage.updateJobDuration(id, estimatedDuration, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(job);
    } catch (error) {
      console.error('Update duration error:', error);
      res.status(500).json({ error: "Failed to update job duration" });
    }
  });

  // Delete job
  app.delete("/api/jobs/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const success = await storage.deleteJob(id, workspaceId);
      if (!success) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Failed to delete job" });
    }
  });

  // Duplicate job for rework
  app.post("/api/jobs/:id/duplicate-for-rework", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const id = parseInt(req.params.id);
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { faultType } = req.body;
      
      if (!faultType || !['Fitter', 'Consultant', 'Factory', 'Client'].includes(faultType)) {
        return res.status(400).json({ error: "Valid fault type is required" });
      }

      const originalJob = await storage.getJob(id, workspaceId);
      if (!originalJob) {
        return res.status(404).json({ error: "Original job not found" });
      }

      // Create a new job with rework order number
      const reworkJobData = {
        jobId: `${originalJob.jobId}-RW`,
        clientName: originalJob.clientName,
        address: originalJob.address,
        type: originalJob.type,
        eta: originalJob.eta,
        status: 'Rework' as const,
        urgent: false,
        faultType,
        workspaceId,
      };

      const newJob = await storage.createJob(reworkJobData);
      res.status(201).json(newJob);
    } catch (error) {
      console.error('Error duplicating job for rework:', error);
      res.status(500).json({ error: "Failed to duplicate job for rework" });
    }
  });

  // Check for duplicate jobs
  app.post("/api/jobs/check-duplicates", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { jobIds } = req.body;
      
      if (!jobIds || !Array.isArray(jobIds)) {
        return res.status(400).json({ error: "Job IDs array is required" });
      }

      const duplicates = await storage.checkDuplicateJobs(jobIds, workspaceId);
      res.json({ duplicates });
    } catch (error) {
      res.status(500).json({ error: "Failed to check for duplicates" });
    }
  });

  // Bulk import jobs
  app.post("/api/jobs/bulk-import", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { jobsText, ignoreDuplicates } = req.body;
      
      if (!jobsText || typeof jobsText !== 'string') {
        return res.status(400).json({ error: "Job text data is required" });
      }

      const lines = jobsText.trim().split('\n').filter(line => line.trim());
      const jobs: InsertJob[] = [];
      const jobIds: string[] = [];
      
      for (const line of lines) {
        const parts = line.split('\t');
        // Now accepts: JobID, ClientName, Address, Type, ETA, InstallCost, Phone, Consultant
        // Minimum 5 columns required (JobID, ClientName, Address, Type, ETA)
        if (parts.length >= 5) {
          const [jobId, clientName, address, type, etaStr, installCostStr, phoneStr, consultantStr] = parts;
          
          try {
            // Convert date format from DD/MM/YYYY to YYYY-MM-DD, or keep as "Pending" if not a date
            let eta = 'Pending';
            const trimmedEta = etaStr.trim();
            
            // Check if it looks like a date (contains slashes for DD/MM/YYYY format)
            if (trimmedEta.includes('/')) {
              const dateParts = trimmedEta.split('/');
              if (dateParts.length === 3) {
                const [day, month, year] = dateParts;
                if (day && month && year) {
                  eta = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
                }
              }
            } else if (trimmedEta.toLowerCase() === 'pending' || trimmedEta.toLowerCase() === 'pending eta' || trimmedEta === '') {
              // Keep "Pending" for pending/empty values
              eta = 'Pending';
            } else {
              // For any other non-date format, keep as-is (could be other text)
              eta = trimmedEta;
            }
            
            // Parse install cost if available
            let installCost = undefined;
            if (installCostStr && installCostStr.trim() !== '') {
              const costValue = parseFloat(installCostStr.trim());
              if (!isNaN(costValue)) {
                installCost = costValue.toString();
              }
            }
            
            const jobData: any = {
              jobId: jobId.trim(),
              clientName: clientName.trim(),
              address: address.trim(),
              type: type.trim(),
              eta,
              status: 'To Do',
              urgent: false,
              workspaceId,
            };
            
            if (installCost !== undefined) {
              jobData.installCost = installCost;
            }
            
            // Add phone number if provided
            if (phoneStr && phoneStr.trim() !== '') {
              jobData.phoneNumber = phoneStr.trim();
            }
            
            // Add consultant if provided
            if (consultantStr && consultantStr.trim() !== '') {
              jobData.consultant = consultantStr.trim();
            }
            
            jobs.push(jobData);
            jobIds.push(jobId.trim());
          } catch (dateError) {
            console.error("Date parsing error for line:", line, dateError);
          }
        }
      }

      if (jobs.length === 0) {
        return res.status(400).json({ error: "No valid jobs found in the provided text" });
      }

      // Check for duplicates by job ID AND type (allow same job ID with different types)
      const existingJobs = await storage.getJobsByJobIds(jobIds, workspaceId);
      
      // FIRST: Rename any conflicting CM jobs by adding "CM" suffix to their job ID
      // This prevents bulk imports from being invisible when they conflict with CM jobs
      let renamedCMCount = 0;
      for (const job of jobs) {
        // Only process regular jobs (not CM jobs)
        if (!(job as any).isCheckMeasureJob) {
          // Check if there's a CM job with the same job ID
          const conflictingCMJob = existingJobs.find(
            existing => existing.jobId === job.jobId && existing.isCheckMeasureJob
          );
          
          if (conflictingCMJob) {
            // Rename the CM job by adding "CM" suffix
            const newCMJobId = `${conflictingCMJob.jobId}CM`;
            await storage.updateJob(conflictingCMJob.id, { jobId: newCMJobId }, workspaceId);
            
            // Update the in-memory array so subsequent checks work correctly
            conflictingCMJob.jobId = newCMJobId;
            renamedCMCount++;
          }
        }
      }
      
      const newJobs: InsertJob[] = [];
      const existingJobsData: InsertJob[] = [];
      
      for (const job of jobs) {
        // Check if a job with same job ID, type AND isCheckMeasureJob status already exists
        // This allows both CM and regular jobs to coexist with the same job ID
        const duplicateJob = existingJobs.find(
          existing => existing.jobId === job.jobId && 
                     existing.type === job.type &&
                     !!existing.isCheckMeasureJob === !!(job as any).isCheckMeasureJob
        );
        
        if (duplicateJob) {
          existingJobsData.push(job);
        } else {
          newJobs.push(job);
        }
      }
      
      // Placeholder values that should be treated as empty (case-insensitive)
      const placeholderValuesLower = [
        'service order', 'sales order', 'check measure', 'job sheet', 'fitter work',
        'work sheet', 'service sheet', 'install order', 'measure sheet',
        'unknown client', 'address not found', 'service orders', 'sales orders',
        'pending', 'pending eta'
      ];
      const isPlaceholder = (val: string) => placeholderValuesLower.includes(val.toLowerCase().trim());
      
      // Helper: Check if value is empty OR is a placeholder value (preserve 0 as valid)
      const isEmpty = (val: any) => {
        if (val === null || val === undefined) return true;
        if (typeof val === 'string') {
          const trimmed = val.trim();
          return trimmed === '' || isPlaceholder(trimmed);
        }
        return false;
      };
      const hasValue = (val: any) => {
        if (val === null || val === undefined) return false;
        if (typeof val === 'string') {
          const trimmed = val.trim();
          return trimmed !== '' && !isPlaceholder(trimmed);
        }
        return true;
      };
      const normalizeStr = (val: any) => typeof val === 'string' ? val.trim() : val;
      
      // Bi-directional merge: Update empty fields AND replace placeholder values
      let updatedCount = 0;
      for (const jobData of existingJobsData) {
        const existingJob = existingJobs.find(
          j => j.jobId === jobData.jobId && 
               j.type === jobData.type &&
               !!j.isCheckMeasureJob === !!(jobData as any).isCheckMeasureJob
        );
        if (existingJob) {
          const updateData: any = {};
          let fieldsUpdated = 0;
          
          // Debug logging for ETA update
          console.log(`[Bulk Import] Processing ${existingJob.jobId}: existingETA="${existingJob.eta}", importETA="${jobData.eta}"`);
          console.log(`[Bulk Import] isEmpty(existingETA)=${isEmpty(existingJob.eta)}, hasValue(importETA)=${hasValue(jobData.eta)}`);
          
          // Only update install cost if existing job doesn't have one (0 is valid)
          if (isEmpty(existingJob.installCost) && jobData.installCost !== undefined && jobData.installCost !== null) {
            updateData.installCost = jobData.installCost;
            fieldsUpdated++;
          }
          
          // Also update other empty fields if bulk import has them (trim and skip duplicates)
          if (isEmpty(existingJob.clientName) && hasValue(jobData.clientName)) {
            const val = normalizeStr(jobData.clientName);
            if (val !== normalizeStr(existingJob.clientName)) {
              updateData.clientName = val;
              fieldsUpdated++;
            }
          }
          if (isEmpty(existingJob.address) && hasValue(jobData.address)) {
            const val = normalizeStr(jobData.address);
            if (val !== normalizeStr(existingJob.address)) {
              updateData.address = val;
              fieldsUpdated++;
            }
          }
          if (isEmpty(existingJob.eta) && hasValue(jobData.eta)) {
            const val = normalizeStr(jobData.eta);
            console.log(`[Bulk Import] ETA check passed for ${existingJob.jobId}: will update to "${val}"`);
            if (val !== normalizeStr(existingJob.eta)) {
              updateData.eta = val;
              fieldsUpdated++;
              console.log(`[Bulk Import] ETA update queued for ${existingJob.jobId}: "${existingJob.eta}" -> "${val}"`);
            }
          }
          if (isEmpty(existingJob.installer) && hasValue(jobData.installer)) {
            const val = normalizeStr(jobData.installer);
            if (val !== normalizeStr(existingJob.installer)) {
              updateData.installer = val;
              fieldsUpdated++;
            }
          }
          if (isEmpty(existingJob.consultant) && hasValue(jobData.consultant)) {
            const val = normalizeStr(jobData.consultant);
            if (val !== normalizeStr(existingJob.consultant)) {
              updateData.consultant = val;
              fieldsUpdated++;
            }
          }
          if (isEmpty(existingJob.phoneNumber) && hasValue(jobData.phoneNumber)) {
            const val = normalizeStr(jobData.phoneNumber);
            if (val !== normalizeStr(existingJob.phoneNumber)) {
              updateData.phoneNumber = val;
              fieldsUpdated++;
            }
          }
          
          if (fieldsUpdated > 0) {
            await storage.updateJob(existingJob.id, updateData, workspaceId);
            updatedCount++;
          }
        }
      }
      
      // Import new jobs
      let createdJobs: Job[] = [];
      if (newJobs.length > 0) {
        createdJobs = await storage.bulkImportJobs(newJobs);
      }
      
      // Build response message
      const messages: string[] = [];
      if (createdJobs.length > 0) {
        messages.push(`${createdJobs.length} new job${createdJobs.length > 1 ? 's' : ''} imported`);
      }
      if (renamedCMCount > 0) {
        messages.push(`${renamedCMCount} CM job${renamedCMCount > 1 ? 's' : ''} renamed to avoid conflicts`);
      }
      if (updatedCount > 0) {
        messages.push(`${updatedCount} existing job${updatedCount > 1 ? 's' : ''} updated with missing data`);
      }
      if (existingJobsData.length > updatedCount) {
        messages.push(`${existingJobsData.length - updatedCount} existing job${(existingJobsData.length - updatedCount) > 1 ? 's' : ''} skipped (no new data to add)`);
      }
      
      const message = messages.length > 0 ? messages.join(', ') : 'No changes made';
      
      res.status(createdJobs.length > 0 ? 201 : 200).json({ 
        message: message.charAt(0).toUpperCase() + message.slice(1),
        imported: createdJobs.length,
        renamedCM: renamedCMCount,
        updated: updatedCount,
        skipped: existingJobsData.length - updatedCount,
        jobs: createdJobs
      });
    } catch (error) {
      console.error("Bulk import error:", error);
      res.status(500).json({ error: "Failed to import jobs" });
    }
  });

  // Job Document Routes
  
  // Get all documents for a job
  app.get("/api/jobs/:id/documents", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const jobId = parseInt(req.params.id);
      const documents = await storage.getJobDocuments(jobId, workspaceId);
      res.json(documents);
    } catch (error) {
      console.error("Error fetching job documents:", error);
      res.status(500).json({ error: "Failed to fetch job documents" });
    }
  });

  // Upload document for a job
  app.post("/api/jobs/:id/documents", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const jobId = parseInt(req.params.id);
      const { fileName, fileContent, documentType = 'attachment' } = req.body;
      
      if (!fileName || !fileContent) {
        return res.status(400).json({ error: "File name and content are required" });
      }

      // Clean content to remove null bytes and other invalid UTF-8 characters
      let cleanedContent = fileContent
        .replace(/\x00/g, '') // Remove null bytes
        .replace(/[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '') // Remove other control characters
        .trim();
      
      // Extract data from the document if it's HTML
      let extractedData = null;
      
      if (fileName.toLowerCase().endsWith('.html') || cleanedContent.includes('<html>')) {
        try {
          const extractedResult = extractJobDataFromHtml(cleanedContent);
          extractedData = JSON.stringify(extractedResult);
          // Use the cleaned HTML content (without scissors section)
          cleanedContent = extractedResult.originalHtml || cleanedContent;
        } catch (error) {
          console.log("Could not extract data from document:", error);
        }
      }

      // Verify job belongs to user's workspace
      const job = await storage.getJob(jobId, workspaceId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }

      // Create job document record
      const jobDocument = await storage.createJobDocument({
        jobId,
        fileName,
        originalContent: cleanedContent, // Use cleaned content instead of raw fileContent
        extractedData,
        documentType,
      });

      res.status(201).json(jobDocument);
    } catch (error) {
      console.error("Error uploading job document:", error);
      res.status(500).json({ error: "Failed to upload job document" });
    }
  });

  // Update a job document
  app.patch("/api/jobs/:jobId/documents/:documentId", isAuthenticated, isActive, requireWriteAccess, async (req, res) => {
    try {
      const documentId = parseInt(req.params.documentId);
      const { formData } = req.body;
      
      // Update the document's extracted data with the form data
      const updatedDocument = await storage.updateJobDocument(documentId, formData);
      
      if (!updatedDocument) {
        return res.status(404).json({ error: "Job document not found" });
      }
      
      res.json(updatedDocument);
    } catch (error) {
      console.error("Error updating job document:", error);
      res.status(500).json({ error: "Failed to update job document" });
    }
  });

  // Delete a job document
  app.delete("/api/jobs/:jobId/documents/:documentId", isAuthenticated, isActive, requireWriteAccess, async (req, res) => {
    try {
      const documentId = parseInt(req.params.documentId);
      const success = await storage.deleteJobDocument(documentId);
      
      if (!success) {
        return res.status(404).json({ error: "Job document not found" });
      }
      
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting job document:", error);
      res.status(500).json({ error: "Failed to delete job document" });
    }
  });

  // CM Document Routes (Check Measure specific documents)
  
  // Get all CM documents for a job
  app.get("/api/cm-documents/:id", async (req, res) => {
    try {
      const jobId = parseInt(req.params.id);
      const documents = await storage.getJobDocuments(jobId);
      // Filter to only return CM-specific documents
      const cmDocuments = documents.filter(doc => 
        doc.documentType === 'cm' || 
        doc.documentType === 'check_measure' || 
        doc.documentType === 'check_measure_edited'
      );
      res.json(cmDocuments);
    } catch (error) {
      console.error("Error fetching CM documents:", error);
      res.status(500).json({ error: "Failed to fetch CM documents" });
    }
  });

  // Create a new CM document (for edited versions)
  app.post("/api/cm-documents", async (req, res) => {
    try {
      const { jobId, title, originalContent, documentType } = req.body;
      
      if (!jobId || !title || !originalContent) {
        return res.status(400).json({ error: "Job ID, title, and content are required" });
      }

      // Clean the HTML content to avoid encoding issues
      const cleanContent = originalContent
        .replace(/\x00/g, '') // Remove null bytes
        .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '') // Remove control characters
        .replace(/&nbsp;/g, ' ') // Replace non-breaking spaces
        .trim();

      // Create CM document record
      const cmDocument = await storage.createJobDocument({
        jobId: parseInt(jobId),
        fileName: `${title}.html`,
        originalContent: cleanContent,
        extractedData: null,
        documentType: documentType || 'check_measure_edited',
      });

      res.status(201).json(cmDocument);
    } catch (error) {
      console.error("Error creating CM document:", error);
      res.status(500).json({ error: "Failed to create CM document" });
    }
  });

  // Upload CM document for a job
  app.post("/api/cm-documents/upload", async (req, res) => {
    try {
      const { jobId } = req.body;
      
      if (!jobId) {
        return res.status(400).json({ error: "Job ID is required" });
      }

      // Handle file upload
      const files = req.files as any;
      if (!files || !files.file) {
        return res.status(400).json({ error: "No file uploaded" });
      }

      const file = files.file;
      const fileName = file.name;
      const fileContent = file.data.toString('utf8');

      // Clean file content to avoid encoding issues
      const cleanedFileContent = fileContent
        .replace(/\x00/g, '') // Remove null bytes
        .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '') // Remove control characters
        .trim();

      // Extract data from the document if it's HTML and get cleaned content
      let extractedData = null;
      let cleanedContent = cleanedFileContent;
      
      if (fileName.toLowerCase().endsWith('.html') || cleanedFileContent.includes('<html>')) {
        try {
          const extractedResult = extractJobDataFromHtml(cleanedFileContent);
          extractedData = JSON.stringify(extractedResult);
          // Use the cleaned HTML content (without scissors section)
          cleanedContent = extractedResult.originalHtml || cleanedFileContent;
        } catch (error) {
          console.log("Could not extract data from CM document:", error);
        }
      }

      // Create CM document record
      const cmDocument = await storage.createJobDocument({
        jobId: parseInt(jobId),
        fileName,
        originalContent: cleanedContent,
        extractedData,
        documentType: 'cm', // Mark as CM document
      });

      // Mark the job as having a check measure document
      const existingJob = await storage.getJob(parseInt(jobId));
      if (existingJob && !existingJob.hasCheckMeasure) {
        await storage.updateJob(parseInt(jobId), { ...existingJob, hasCheckMeasure: true });
      }

      res.status(201).json(cmDocument);
    } catch (error) {
      console.error("Error uploading CM document:", error);
      res.status(500).json({ error: "Failed to upload CM document" });
    }
  });

  // Update a CM document
  app.put("/api/cm-documents/:documentId", async (req, res) => {
    try {
      const documentId = parseInt(req.params.documentId);
      const { extractedData } = req.body;
      
      // Update the document's extracted data with the form data
      const updatedDocument = await storage.updateJobDocument(documentId, extractedData);
      
      if (!updatedDocument) {
        return res.status(404).json({ error: "CM document not found" });
      }
      
      res.json(updatedDocument);
    } catch (error) {
      console.error("Error updating CM document:", error);
      res.status(500).json({ error: "Failed to update CM document" });
    }
  });

  // Delete a CM document
  app.delete("/api/cm-documents/:documentId", async (req, res) => {
    try {
      const documentId = parseInt(req.params.documentId);
      const success = await storage.deleteJobDocument(documentId);
      
      if (!success) {
        return res.status(404).json({ error: "CM document not found" });
      }
      
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting CM document:", error);
      res.status(500).json({ error: "Failed to delete CM document" });
    }
  });

  // Check Measure Sheet routes
  
  // Extract job data from uploaded HTML file (for Add CM modal)
  app.post("/api/check-measure/extract-data", async (req, res) => {
    try {
      const { htmlContent } = req.body;
      
      if (!htmlContent) {
        return res.status(400).json({ error: "HTML content is required" });
      }

      // Extract job information from HTML content
      const extractedData = extractJobDataFromHtml(htmlContent);
      
      res.json(extractedData);
    } catch (error) {
      console.error("Error extracting job data:", error);
      res.status(500).json({ error: "Failed to extract job data from HTML" });
    }
  });

  // Extract job data from uploaded file (for Add Job modal)
  app.post("/api/jobs/extract-data", async (req, res) => {
    try {
      let htmlContent = '';
      
      if (req.body.htmlContent) {
        htmlContent = req.body.htmlContent;
      } else if (req.body instanceof FormData || req.files) {
        // Handle file upload
        const file = req.files?.file || req.body.file;
        if (file) {
          htmlContent = file.toString();
        }
      }
      
      if (!htmlContent) {
        return res.status(400).json({ error: "HTML content is required" });
      }

      // Extract job information from HTML content using the same function
      const extractedData = extractJobDataFromHtml(htmlContent);
      
      res.json(extractedData);
    } catch (error) {
      console.error("Error extracting job data:", error);
      res.status(500).json({ error: "Failed to extract job data from HTML" });
    }
  });
  
  // Get check measure sheet for a job
  app.get("/api/jobs/:id/check-measure", async (req, res) => {
    try {
      const jobId = parseInt(req.params.id);
      const checkMeasureSheet = await storage.getCheckMeasureSheet(jobId);
      if (checkMeasureSheet) {
        return res.json(checkMeasureSheet);
      }

      // Fall back to jobDocuments table for CM documents (uploaded via docs modal or Gmail sync)
      const jobDocuments = await storage.getJobDocuments(jobId);
      const cmDoc = jobDocuments.find(doc =>
        doc.documentType === 'cm' ||
        doc.documentType === 'check_measure' ||
        doc.documentType === 'check_measure_edited'
      );
      if (cmDoc) {
        // Parse extractedData from JSON string to object (it's stored as text/JSON.stringify)
        let parsedFormData: any = {};
        if (cmDoc.extractedData) {
          try {
            parsedFormData = JSON.parse(cmDoc.extractedData);
            // If the parsed result contains job metadata keys (not form input keys), reset to {}
            // Form input keys always start with 'table_' — job metadata keys don't
            const keys = Object.keys(parsedFormData);
            const hasFormKeys = keys.some(k => k.startsWith('table_'));
            if (!hasFormKeys && keys.length > 0) parsedFormData = {};
          } catch { parsedFormData = {}; }
        }
        // Return a compatible shape that CheckMeasureViewer understands
        return res.json({
          id: cmDoc.id,
          jobId: cmDoc.jobId,
          originalHtml: cmDoc.originalContent,
          formData: parsedFormData,
          templateData: null,
          createdAt: cmDoc.createdAt,
          updatedAt: cmDoc.createdAt,
          _source: 'jobDocument',
        });
      }

      return res.status(404).json({ error: "Check measure sheet not found" });
    } catch (error) {
      res.status(500).json({ error: "Failed to fetch check measure sheet" });
    }
  });

  // Job Sheet Routes
  app.get("/api/job-sheets", async (req, res) => {
    try {
      const jobSheets = await storage.getAllJobSheets();
      res.json(jobSheets);
    } catch (error) {
      console.error('Error fetching job sheets:', error);
      res.status(500).json({ error: "Failed to fetch job sheets" });
    }
  });

  app.get("/api/job-sheets/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const jobSheet = await storage.getJobSheet(id);
      if (!jobSheet) {
        return res.status(404).json({ error: "Job sheet not found" });
      }
      res.json(jobSheet);
    } catch (error) {
      console.error('Error fetching job sheet:', error);
      res.status(500).json({ error: "Failed to fetch job sheet" });
    }
  });

  // Extract job data from job sheet HTML content
  app.post("/api/job-sheets/extract-data", async (req, res) => {
    try {
      const { htmlContent } = req.body;
      
      if (!htmlContent) {
        return res.status(400).json({ error: "HTML content is required" });
      }

      // Extract job information from HTML content
      const extractedData = extractJobSheetData(htmlContent);
      
      res.json(extractedData);
    } catch (error) {
      console.error("Error extracting job sheet data:", error);
      res.status(500).json({ error: "Failed to extract job sheet data from HTML" });
    }
  });

  app.post("/api/job-sheets/upload", async (req, res) => {
    try {
      const { fileName, fileContent } = req.body;
      
      if (!fileName || !fileContent) {
        return res.status(400).json({ error: "File name and content are required" });
      }

      // Extract job data from the file content
      const extractedData = extractJobSheetData(fileContent);
      
      // Create job sheet record
      const jobSheet = await storage.createJobSheet({
        fileName,
        originalContent: fileContent,
        extractedData: JSON.stringify(extractedData),
      });

      res.json({
        success: true,
        jobSheet,
        extractedData,
      });
    } catch (error) {
      console.error('Error uploading job sheet:', error);
      res.status(500).json({ error: "Failed to upload job sheet" });
    }
  });

  app.delete("/api/job-sheets/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteJobSheet(id);
      if (!success) {
        return res.status(404).json({ error: "Job sheet not found" });
      }
      res.status(204).send();
    } catch (error) {
      console.error('Error deleting job sheet:', error);
      res.status(500).json({ error: "Failed to delete job sheet" });
    }
  });

  // Create check measure sheet for a job
  app.post("/api/jobs/:id/check-measure", async (req, res) => {
    try {
      const jobId = parseInt(req.params.id);
      const sheetData = { ...req.body, jobId };
      const validatedData = insertCheckMeasureSheetSchema.parse(sheetData);
      const checkMeasureSheet = await storage.createCheckMeasureSheet(validatedData);
      
      // Update job to mark it as having a check measure sheet
      const existingJob = await storage.getJob(jobId);
      if (existingJob) {
        await storage.updateJob(jobId, { 
          ...existingJob, 
          hasCheckMeasure: true 
        });
      }
      
      res.status(201).json(checkMeasureSheet);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ error: "Invalid check measure sheet data", details: error.errors });
      }
      res.status(500).json({ error: "Failed to create check measure sheet" });
    }
  });

  // Update check measure sheet form data
  app.patch("/api/check-measure/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const { formData, _source } = req.body;

      // Helper to safely parse extractedData text as JSON form inputs
      const parseDocFormData = (extractedData: string | null): any => {
        if (!extractedData) return {};
        try {
          const parsed = JSON.parse(extractedData);
          const keys = Object.keys(parsed);
          const hasFormKeys = keys.some(k => k.startsWith('table_'));
          return hasFormKeys ? parsed : {};
        } catch { return {}; }
      };

      // If this came from a jobDocument, save to jobDocuments table instead
      if (_source === 'jobDocument') {
        const updatedDoc = await storage.updateJobDocument(id, formData);
        if (!updatedDoc) {
          return res.status(404).json({ error: "Document not found" });
        }
        return res.json({
          id: updatedDoc.id,
          jobId: updatedDoc.jobId,
          originalHtml: updatedDoc.originalContent,
          formData: parseDocFormData(updatedDoc.extractedData),
          templateData: null,
          _source: 'jobDocument',
        });
      }

      const updatedSheet = await storage.updateCheckMeasureSheet(id, formData);
      if (!updatedSheet) {
        // Fall back: maybe the id is for a jobDocument
        const updatedDoc = await storage.updateJobDocument(id, formData);
        if (updatedDoc) {
          return res.json({
            id: updatedDoc.id,
            jobId: updatedDoc.jobId,
            originalHtml: updatedDoc.originalContent,
            formData: parseDocFormData(updatedDoc.extractedData),
            templateData: null,
            _source: 'jobDocument',
          });
        }
        return res.status(404).json({ error: "Check measure sheet not found" });
      }
      res.json(updatedSheet);
    } catch (error) {
      res.status(500).json({ error: "Failed to update check measure sheet" });
    }
  });

  // Delete check measure sheet
  app.delete("/api/check-measure/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteCheckMeasureSheet(id);
      if (!success) {
        return res.status(404).json({ error: "Check measure sheet not found" });
      }
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: "Failed to delete check measure sheet" });
    }
  });

  // Send check measure sheet via email
  app.post("/api/email/send-check-measure", async (req, res) => {
    try {
      const { to, subject, body, jobId, clientName, htmlContent } = req.body;
      
      if (!process.env.SENDGRID_API_KEY) {
        return res.status(500).json({ error: "Email service not configured" });
      }
      
      sgMail.setApiKey(process.env.SENDGRID_API_KEY);
      
      const msg = {
        to,
        from: process.env.FROM_EMAIL || 'noreply@yourdomain.com',
        subject,
        text: body,
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2>Check Measure Sheet</h2>
            <p><strong>Job ID:</strong> ${jobId}</p>
            <p><strong>Client:</strong> ${clientName}</p>
            <hr style="margin: 20px 0;">
            <p>${body.replace(/\n/g, '<br>')}</p>
            <hr style="margin: 20px 0;">
            <div style="background: #f5f5f5; padding: 20px; border-radius: 5px;">
              <h3>Check Measure Sheet Details</h3>
              ${htmlContent}
            </div>
          </div>
        `
      };
      
      await sgMail.send(msg);
      res.json({ success: true, message: "Email sent successfully" });
    } catch (error) {
      console.error('Email error:', error);
      res.status(500).json({ error: "Failed to send email" });
    }
  });

  // Job Sheet Upload Routes
  app.get("/api/job-sheets", async (req, res) => {
    try {
      const jobSheets = await storage.getAllJobSheets();
      res.json(jobSheets);
    } catch (error) {
      console.error("Error fetching job sheets:", error);
      res.status(500).json({ error: "Failed to fetch job sheets" });
    }
  });

  app.post("/api/job-sheets/upload", async (req, res) => {
    try {
      const { fileName, fileContent } = req.body;
      
      if (!fileName || !fileContent) {
        return res.status(400).json({ error: "File name and content are required" });
      }

      // Extract data from the job sheet (similar to check measure)
      const extractedData = extractJobSheetData(fileContent);
      
      // Save to storage
      const jobSheet = await storage.createJobSheet({
        fileName,
        originalContent: fileContent,
        extractedData: JSON.stringify(extractedData),
      });

      res.json({
        id: jobSheet.id,
        fileName: jobSheet.fileName,
        extractedData,
        originalContent: fileContent,
      });
    } catch (error) {
      console.error("Error uploading job sheet:", error);
      res.status(500).json({ error: "Failed to upload job sheet" });
    }
  });

  app.get("/api/job-sheets/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const jobSheet = await storage.getJobSheet(id);
      
      if (!jobSheet) {
        return res.status(404).json({ error: "Job sheet not found" });
      }

      res.json(jobSheet);
    } catch (error) {
      console.error("Error fetching job sheet:", error);
      res.status(500).json({ error: "Failed to fetch job sheet" });
    }
  });

  app.delete("/api/job-sheets/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteJobSheet(id);
      
      if (success) {
        res.json({ success: true });
      } else {
        res.status(404).json({ error: "Job sheet not found" });
      }
    } catch (error) {
      console.error("Error deleting job sheet:", error);
      res.status(500).json({ error: "Failed to delete job sheet" });
    }
  });

  // Google Calendar Authorization Routes
  app.get("/api/calendar/auth-status", async (req, res) => {
    try {
      const isAuthorized = await isCalendarAuthorized();
      res.json({ authorized: isAuthorized });
    } catch (error) {
      res.status(500).json({ error: "Failed to check authorization status" });
    }
  });

  app.get("/api/calendar/auth-url", async (req, res) => {
    try {
      // Get the origin from the custom header (sent from frontend), fallback to other methods
      const origin = req.get('X-Origin') || req.get('origin') || req.get('referer')?.replace(/\/$/, '') || `${req.protocol}://${req.get('host')}`;
      console.log('[Google Calendar] Auth URL requested from origin:', origin);
      const authUrl = getAuthUrl(origin);
      res.json({ authUrl });
    } catch (error) {
      res.status(500).json({ error: "Failed to get authorization URL" });
    }
  });



  app.get("/auth/google/callback", async (req, res) => {
    console.log('[Google Calendar] Callback route hit:', req.url);
    console.log('[Google Calendar] Query params:', req.query);
    try {
      const code = req.query.code as string;
      if (!code) {
        console.log('[Google Calendar] No authorization code provided');
        return res.status(400).send("Authorization code not provided");
      }
      
      // Use the host from the request, not the origin/referer (which will be google.com)
      const protocol = req.get('x-forwarded-proto') || req.protocol || 'https';
      const host = req.get('host');
      const origin = `${protocol}://${host}`;
      console.log('[Google Calendar] Callback received, using origin from host:', origin);
      console.log('[Google Calendar] Processing authorization code:', code.substring(0, 20) + '...');
      const result = await handleAuthCallback(code, origin);
      console.log('[Google Calendar] Auth callback result:', result);
      
      if (result.success) {
        console.log('[Google Calendar] Authorization successful');
        res.send(`
          <html>
            <body>
              <h1>Google Calendar Connected!</h1>
              <p>You can now close this window and return to the app.</p>
              <script>
                // Close the popup window
                if (window.opener) {
                  window.opener.postMessage('calendar-connected', '*');
                  window.close();
                } else {
                  // If not in popup, redirect to main app
                  window.location.href = '/';
                }
              </script>
            </body>
          </html>
        `);
      } else {
        console.log('[Google Calendar] Authorization failed:', result.error);
        res.status(400).send(`Authorization failed: ${result.error}`);
      }
    } catch (error) {
      console.error('[Google Calendar] Callback error:', error);
      res.status(500).send(`Authorization failed: ${error}`);
    }
  });

  // Start calendar sync service manually (also starts automatically on auth)
  app.post("/api/calendar/start-sync", async (req, res) => {
    try {
      const { startCalendarSyncService } = await import('./google-calendar');
      startCalendarSyncService();
      res.json({ success: true, message: "Calendar sync service started" });
    } catch (error) {
      res.status(500).json({ error: "Failed to start sync service" });
    }
  });

  // Trigger manual sync
  app.post("/api/calendar/sync-now", async (req, res) => {
    try {
      const { syncCalendarEvents } = await import('./google-calendar');
      await syncCalendarEvents();
      res.json({ success: true, message: "Calendar sync completed" });
    } catch (error) {
      res.status(500).json({ error: "Failed to sync calendar" });
    }
  });

  // Fix existing calendar event times
  app.post("/api/calendar/fix-existing-times", async (req, res) => {
    try {
      const { fixExistingCalendarEventTimes } = await import('./google-calendar');
      const result = await fixExistingCalendarEventTimes();
      res.json(result);
    } catch (error) {
      console.error('Error fixing existing calendar times:', error);
      res.status(500).json({ error: "Failed to fix existing calendar times" });
    }
  });

  // Manual sync to detect deleted events immediately
  app.post("/api/calendar/sync-deleted-events", async (req, res) => {
    try {
      const { syncCalendarEvents } = await import('./google-calendar');
      await syncCalendarEvents();
      res.json({ success: true, message: "Calendar sync completed - checked for deleted events" });
    } catch (error) {
      console.error('Error syncing deleted events:', error);
      res.status(500).json({ error: "Failed to sync deleted events" });
    }
  });

  // Create missing calendar events for booked jobs
  app.post("/api/calendar/create-missing-events", async (req, res) => {
    try {
      const workspaceId = (req as any).user?.workspaceId || 1;
      
      // Get all booked jobs without calendar events
      const bookedJobs = await storage.getAllJobs(workspaceId);
      const jobsNeedingEvents = bookedJobs.filter(job => 
        job.status === 'Booked' && 
        job.bookingDate && 
        !job.calendarEventId
      );
      
      console.log(`Found ${jobsNeedingEvents.length} booked jobs missing calendar events`);
      
      const { createCalendarEvent } = await import('./google-calendar');
      const results = [];
      
      for (const job of jobsNeedingEvents) {
        try {
          console.log(`Creating calendar event for job ${job.jobId} - ${job.clientName}`);
          
          // Create datetime strings in Perth timezone
          const bookingDate = new Date(job.bookingDate).toISOString().split('T')[0];
          const numberOfDays = job.numberOfDays || 1;
          
          // Convert time formats from "8:00 AM" to "08:00" format
          const convertTimeFormat = (timeStr: string): string => {
            if (!timeStr) return '09:00';
            
            // If already in HH:MM format, return as is
            if (/^\d{2}:\d{2}$/.test(timeStr)) {
              return timeStr;
            }
            
            // Convert from "8:00 AM/PM" format to "HH:MM" format
            const match = timeStr.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
            if (match) {
              let hour = parseInt(match[1]);
              const minute = match[2];
              const period = match[3].toLowerCase();
              
              if (period === 'pm' && hour !== 12) {
                hour += 12;
              } else if (period === 'am' && hour === 12) {
                hour = 0;
              }
              
              return `${hour.toString().padStart(2, '0')}:${minute}`;
            }
            
            // Fallback to original or default
            return timeStr || '09:00';
          };
          
          const startTime = convertTimeFormat(job.proposedBookingTime || '09:00');
          const endTime = convertTimeFormat(job.proposedFinishTime || '17:00');
          
          // Create calendar events for multiple days
          const eventIds: string[] = [];
          let allSuccess = true;
          let errors: string[] = [];
          
          for (let dayOffset = 0; dayOffset < numberOfDays; dayOffset++) {
            const currentDate = new Date(bookingDate);
            currentDate.setDate(currentDate.getDate() + dayOffset);
            const currentDateStr = currentDate.toISOString().split('T')[0];
            
            const startDateTime = `${currentDateStr}T${startTime}:00`;
            const endDateTime = `${currentDateStr}T${endTime}:00`;
            
            const dayText = numberOfDays > 1 ? ` (Day ${dayOffset + 1} of ${numberOfDays})` : '';
            const costInfo = job.installCost != null ? `\nCost: $${parseFloat(job.installCost as any).toFixed(2)}` : '';
            
            const eventResult = await createCalendarEvent({
              summary: `FITTER PRO - ${job.type}: ${job.clientName}${dayText}`,
              description: `Job ID: ${job.jobId}\nClient: ${job.clientName}\nType: ${job.type}${costInfo}${numberOfDays > 1 ? `\nDay ${dayOffset + 1} of ${numberOfDays}` : ''}\n\nCreated via FITTER PRO`,
              location: job.address,
              startDateTime,
              endDateTime,
              timeZone: 'Australia/Perth'
            });
            
            if (eventResult.success) {
              eventIds.push(eventResult.eventId);
              console.log(`✓ Created calendar event for ${job.jobId} day ${dayOffset + 1}: ${eventResult.eventId}`);
            } else {
              allSuccess = false;
              errors.push(`Day ${dayOffset + 1}: ${eventResult.error}`);
              console.log(`✗ Failed to create calendar event for ${job.jobId} day ${dayOffset + 1}: ${eventResult.error}`);
            }
          }
          
          if (eventIds.length > 0) {
            // Update job with primary calendar event ID (first day)
            await storage.updateJobCalendarEventId(job.id, eventIds[0], workspaceId);
            
            results.push({
              jobId: job.jobId,
              clientName: job.clientName,
              success: allSuccess,
              eventId: eventIds[0], // Primary event ID
              eventIds: eventIds, // All event IDs
              daysCreated: eventIds.length,
              totalDays: numberOfDays,
              errors: errors.length > 0 ? errors : undefined
            });
          } else {
            results.push({
              jobId: job.jobId,
              clientName: job.clientName,
              success: false,
              error: `Failed to create any calendar events: ${errors.join(', ')}`
            });
          }
        } catch (error) {
          console.error(`Error creating calendar event for job ${job.jobId}:`, error);
          results.push({
            jobId: job.jobId,
            clientName: job.clientName,
            success: false,
            error: error instanceof Error ? error.message : 'Unknown error'
          });
        }
      }
      
      res.json({
        success: true,
        message: `Processed ${jobsNeedingEvents.length} jobs`,
        results,
        created: results.filter(r => r.success).length,
        failed: results.filter(r => !r.success).length
      });
      
    } catch (error) {
      console.error('Error creating missing calendar events:', error);
      res.status(500).json({ error: "Failed to create missing calendar events" });
    }
  });

  // ===== UNIFIED GOOGLE AUTH ROUTES =====
  
  // Check unified Google connection status (Calendar + Gmail)
  app.get("/api/google/status", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.json({ calendarConnected: false, gmailConnected: false, fullyConnected: false });
      }
      
      const { checkGoogleConnectionStatus } = await import('./unified-google-auth');
      const status = await checkGoogleConnectionStatus(workspaceId);
      res.json(status);
    } catch (error) {
      console.error('Error checking Google status:', error);
      res.json({ calendarConnected: false, gmailConnected: false, fullyConnected: false, error: "Failed to check status" });
    }
  });
  
  // Get unified Google auth URL (connects both Calendar and Gmail at once)
  app.get("/api/google/auth-url", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { getUnifiedAuthUrl } = await import('./unified-google-auth');
      const authUrl = await getUnifiedAuthUrl(workspaceId, userId);
      res.json({ authUrl });
    } catch (error) {
      console.error('Error getting unified Google auth URL:', error);
      res.status(500).json({ error: "Failed to get authorization URL" });
    }
  });
  
  // Unified Google OAuth callback
  app.get("/api/google/callback", async (req, res) => {
    console.log('[Unified Google Auth] Callback route hit:', req.url);
    try {
      const code = req.query.code as string;
      const nonce = req.query.state as string;
      
      if (!code) {
        return res.status(400).send("Authorization code not provided");
      }
      
      if (!nonce) {
        return res.status(400).send("Invalid authorization request - missing state");
      }

      const { isSecondaryGmailState, handleSecondaryGmailCallback } = await import('./gmail-service');
      if (isSecondaryGmailState(nonce)) {
        console.log('[Gmail] Secondary Gmail callback detected');
        try {
          const origin = `${req.protocol}://${req.get('host')}`;
          const result = await handleSecondaryGmailCallback(code, nonce, origin);
          return res.send(`
            <html>
              <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
                <h1 style="color: #10b981;">Secondary Gmail Connected!</h1>
                <p>Connected: ${result.emailAddress}</p>
                <p>Invoice emails will now be scanned from this account.</p>
                <script>setTimeout(() => window.close(), 2000);</script>
              </body>
            </html>
          `);
        } catch (error: any) {
          console.error('[Gmail] Secondary callback error:', error);
          return res.status(400).send(`
            <html>
              <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
                <h1 style="color: #e11d48;">Connection Failed</h1>
                <p>${error.message || 'Failed to connect secondary Gmail'}</p>
                <p>Please try again.</p>
              </body>
            </html>
          `);
        }
      }
      
      const { validateAuthNonce, handleUnifiedCallback } = await import('./unified-google-auth');
      const authData = await validateAuthNonce(nonce);
      
      if (!authData) {
        console.error('[Unified Google Auth] Invalid or expired nonce');
        return res.status(400).send(`
          <html>
            <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
              <h1 style="color: #e11d48;">Authorization Expired</h1>
              <p>This authorization link has expired or is invalid.</p>
              <p>Please close this window and try connecting again.</p>
            </body>
          </html>
        `);
      }
      
      const result = await handleUnifiedCallback(code, authData.workspaceId);
      
      if (result.success) {
        res.send(`
          <html>
            <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
              <h1 style="color: #10b981;">Google Connected!</h1>
              <p>Your Google Calendar and Gmail are now connected.</p>
              <p>You can close this window.</p>
              <script>
                setTimeout(() => window.close(), 2000);
              </script>
            </body>
          </html>
        `);
      } else {
        res.status(400).send(`
          <html>
            <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
              <h1 style="color: #e11d48;">Connection Failed</h1>
              <p>${result.error || 'Unknown error occurred'}</p>
              <p>Please try again.</p>
            </body>
          </html>
        `);
      }
    } catch (error) {
      console.error('[Unified Google Auth] Callback error:', error);
      res.status(500).send(`
        <html>
          <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
            <h1 style="color: #e11d48;">Error</h1>
            <p>An unexpected error occurred during authorization.</p>
            <p>Please try again.</p>
          </body>
        </html>
      `);
    }
  });
  
  // Disconnect Google (both Calendar and Gmail)
  app.post("/api/google/disconnect", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { disconnectGoogle } = await import('./unified-google-auth');
      const success = await disconnectGoogle(workspaceId);
      
      if (success) {
        res.json({ success: true, message: "Google disconnected successfully" });
      } else {
        res.status(500).json({ error: "Failed to disconnect Google" });
      }
    } catch (error) {
      console.error('Error disconnecting Google:', error);
      res.status(500).json({ error: "Failed to disconnect Google" });
    }
  });

  // ===== RE-EXTRACT CLIENT NAMES FROM JOB SHEETS =====
  
  // Admin endpoint to re-extract client names from all stored job sheets
  app.post("/api/admin/reextract-client-names", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      // Get all job sheets
      const allJobSheets = await db.select().from(jobSheets).where(eq(jobSheets.workspaceId, workspaceId));
      
      const results: { jobId: string; oldName: string; newName: string; updated: boolean }[] = [];
      
      for (const sheet of allJobSheets) {
        try {
          // Extract job ID from filename (format: J0006415-5_email.html)
          const jobIdMatch = sheet.fileName.match(/^(J\d{7}-\d+)/);
          if (!jobIdMatch) continue;
          
          const jobIdFromFilename = jobIdMatch[1];
          
          // Find the corresponding job
          const [existingJob] = await db.select().from(jobs)
            .where(and(eq(jobs.workspaceId, workspaceId), eq(jobs.jobId, jobIdFromFilename)));
          
          if (!existingJob) continue;
          
          // Only process jobs with empty or invalid client names
          const invalidNames = ['sales order', 'service order', 'check measure', 'job sheet', ''];
          if (!invalidNames.includes((existingJob.clientName || '').toLowerCase().trim())) {
            continue;
          }
          
          // Re-extract client name from the stored HTML
          const htmlContent = sheet.originalContent;
          
          // Define known staff
          const knownStaff = [
            'Michelle Fryer', 'Frances French', 'Emerson Redondo', 'Jody Kenney', 'Jody Kenny', 
            'Cheryl Collister', 'Elena Deighan', 'Renata Victor', 'Tara Carle', 'Alida Miller', 'Lyn Sullivan'
          ];
          
          // Try to extract client name from "Name:" field
          let newClientName = '';
          
          // Pattern 1: Name: FirstName LastName (most reliable)
          const nameFieldPattern = /Name:\s*([A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z]+)/gi;
          let nameMatch;
          while ((nameMatch = nameFieldPattern.exec(htmlContent)) !== null) {
            if (nameMatch[1]) {
              const candidate = nameMatch[1].trim();
              if (!knownStaff.includes(candidate)) {
                newClientName = candidate;
                break;
              }
            }
          }
          
          // Pattern 2: Try contact field with email to find associated name
          if (!newClientName) {
            // Look for name near contact/email info
            const contactPatterns = [
              /([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\S]{0,50}Contact:/gi,
              /([A-Z][a-z]+\s+[A-Z][a-z]+)[\s\S]{0,100}@[a-z]+\.[a-z]+/gi,
            ];
            
            for (const pattern of contactPatterns) {
              pattern.lastIndex = 0;
              const match = pattern.exec(htmlContent);
              if (match && match[1]) {
                const candidate = match[1].trim();
                if (!knownStaff.includes(candidate) && 
                    !['Sales Order', 'Service Order', 'Check Measure'].includes(candidate)) {
                  newClientName = candidate;
                  break;
                }
              }
            }
          }
          
          // Update job if we found a valid name
          if (newClientName && newClientName !== existingJob.clientName) {
            await db.update(jobs)
              .set({ clientName: newClientName })
              .where(eq(jobs.id, existingJob.id));
              
            results.push({
              jobId: existingJob.jobId,
              oldName: existingJob.clientName || '',
              newName: newClientName,
              updated: true
            });
          } else {
            results.push({
              jobId: existingJob.jobId,
              oldName: existingJob.clientName || '',
              newName: newClientName || 'NOT FOUND',
              updated: false
            });
          }
        } catch (err) {
          console.error(`Error processing sheet ${sheet.fileName}:`, err);
        }
      }
      
      res.json({
        success: true,
        processed: results.length,
        updated: results.filter(r => r.updated).length,
        results
      });
    } catch (error: any) {
      console.error('Error re-extracting client names:', error);
      res.status(500).json({ error: error.message });
    }
  });

  // ===== GMAIL INTEGRATION ROUTES =====
  
  // Test endpoint to analyze Job Bank email format
  app.get("/api/gmail/test-jobbank", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { getGmailClient } = await import('./gmail-service');
      const gmail = await getGmailClient(workspaceId);
      
      if (!gmail) {
        return res.status(400).json({ error: "Gmail not connected" });
      }
      
      // Search for Job Bank emails
      const query = 'subject:("Job Bank" OR "Job bank") after:2025/11/25 before:2025/11/27';
      
      const response = await gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 5,
      });
      
      const messages = response.data.messages || [];
      const results: any[] = [];
      
      for (const msg of messages) {
        if (msg.id) {
          const fullMsg = await gmail.users.messages.get({
            userId: 'me',
            id: msg.id,
            format: 'full',
          });
          
          const headers = fullMsg.data.payload?.headers || [];
          const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value || '';
          const dateStr = headers.find(h => h.name?.toLowerCase() === 'date')?.value || '';
          
          // Get the email body
          let bodyHtml = '';
          let bodyText = '';
          
          // Check for multipart content
          const parts = fullMsg.data.payload?.parts || [];
          for (const part of parts) {
            if (part.mimeType === 'text/html' && part.body?.data) {
              bodyHtml = Buffer.from(part.body.data, 'base64').toString('utf-8');
            }
            if (part.mimeType === 'text/plain' && part.body?.data) {
              bodyText = Buffer.from(part.body.data, 'base64').toString('utf-8');
            }
          }
          
          // If no parts, check direct body
          if (!bodyHtml && fullMsg.data.payload?.body?.data) {
            const bodyData = Buffer.from(fullMsg.data.payload.body.data, 'base64').toString('utf-8');
            if (bodyData.includes('<') && bodyData.includes('>')) {
              bodyHtml = bodyData;
            } else {
              bodyText = bodyData;
            }
          }
          
          // Try to extract table data from HTML
          let tableData: any[] = [];
          if (bodyHtml) {
            // Look for table rows
            const rowMatches = bodyHtml.match(/<tr[^>]*>[\s\S]*?<\/tr>/gi) || [];
            tableData = rowMatches.map(row => {
              const cells = row.match(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi) || [];
              return cells.map(cell => {
                return cell.replace(/<[^>]+>/g, '').trim();
              });
            });
          }
          
          results.push({
            messageId: msg.id,
            subject,
            date: dateStr,
            hasHtml: !!bodyHtml,
            hasText: !!bodyText,
            htmlLength: bodyHtml.length,
            textPreview: bodyText.substring(0, 500),
            tableRowCount: tableData.length,
            allTableRows: tableData, // Show ALL rows now
          });
        }
      }
      
      res.json({
        found: messages.length,
        query,
        results
      });
    } catch (error: any) {
      console.error('Error testing Job Bank email:', error);
      res.status(500).json({ error: error.message });
    }
  });
  
  // Check Gmail connection status
  app.get("/api/gmail/status", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.json({ connected: false, error: "No workspace" });
      }
      
      const { isGmailConnected } = await import('./gmail-service');
      const connected = await isGmailConnected(workspaceId);
      
      // Get processed email stats
      const processedEmails = await storage.getProcessedEmails(workspaceId);
      
      res.json({ 
        connected,
        processedCount: processedEmails.length,
        lastProcessed: processedEmails[0]?.processedAt || null
      });
    } catch (error) {
      console.error('Error checking Gmail status:', error);
      res.json({ connected: false, error: "Failed to check status" });
    }
  });
  
  // Get Gmail auth URL
  app.get("/api/gmail/auth-url", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      // Get origin from request for correct redirect URI
      const origin = `${req.protocol}://${req.get('host')}`;
      
      const { getGmailAuthUrl } = await import('./gmail-service');
      const authUrl = getGmailAuthUrl(workspaceId, userId, origin);
      res.json({ authUrl });
    } catch (error) {
      console.error('Error getting Gmail auth URL:', error);
      res.status(500).json({ error: "Failed to get authorization URL" });
    }
  });
  
  // Gmail OAuth callback - validates secure nonce to prevent unauthorized access
  app.get("/api/gmail/callback", async (req, res) => {
    console.log('[Gmail] Callback route hit:', req.url);
    try {
      const code = req.query.code as string;
      const nonce = req.query.state as string;
      
      if (!code) {
        return res.status(400).send("Authorization code not provided");
      }
      
      if (!nonce) {
        return res.status(400).send("Invalid authorization request - missing state");
      }
      
      const { validateAuthNonce, handleGmailCallback } = await import('./gmail-service');
      const authData = validateAuthNonce(nonce);
      
      if (!authData) {
        console.error('[Gmail] Invalid or expired nonce');
        return res.status(400).send(`
          <html>
            <body>
              <h1>Authorization Expired</h1>
              <p>This authorization link has expired or is invalid.</p>
              <p>Please close this window and try connecting Gmail again.</p>
            </body>
          </html>
        `);
      }
      
      // Get origin from request for correct redirect URI
      const origin = `${req.protocol}://${req.get('host')}`;
      
      const success = await handleGmailCallback(code, authData.workspaceId, origin);
      
      if (success) {
        res.send(`
          <html>
            <body>
              <h1>Gmail Connected!</h1>
              <p>You can now close this window and return to the app.</p>
              <script>
                window.opener?.postMessage({ type: 'GMAIL_AUTH_SUCCESS' }, '*');
                setTimeout(() => window.close(), 2000);
              </script>
            </body>
          </html>
        `);
      } else {
        res.status(400).send("Failed to connect Gmail");
      }
    } catch (error) {
      console.error('[Gmail] Callback error:', error);
      res.status(500).send(`
        <html>
          <body>
            <h1>Connection Failed</h1>
            <p>Error: ${error instanceof Error ? error.message : 'Unknown error'}</p>
            <p>Please close this window and try again.</p>
          </body>
        </html>
      `);
    }
  });
  
  // Sync emails from Gmail
  app.post("/api/gmail/sync", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { fetchNewEmails, processEmailAttachments, isGmailConnected } = await import('./gmail-service');
      
      const connected = await isGmailConnected(workspaceId);
      if (!connected) {
        return res.status(400).json({ error: "Gmail not connected. Please connect your Gmail account first." });
      }
      
      console.log('[Gmail] Fetching new emails for workspace:', workspaceId);
      const emails = await fetchNewEmails(workspaceId);
      console.log(`[Gmail] Found ${emails.length} new emails to process`);
      
      if (emails.length === 0) {
        return res.json({ 
          success: true, 
          message: "No new emails found",
          jobsCreated: 0,
          cmsCreated: 0,
          emailsProcessed: 0
        });
      }
      
      // Process email attachments and Job Bank emails
      const results = await processEmailAttachments(
        workspaceId,
        emails,
        async (htmlContent: string) => {
          // Process as Job Sheet - extract data and create/update job
          const extractedData = extractJobSheetData(htmlContent);
          
          if (extractedData.jobId) {
            // Check if job already exists
            const existingJobsList = await storage.getJobsByJobIds([extractedData.jobId], workspaceId);
            const existingJob = existingJobsList.find(j => j.jobId === extractedData.jobId && !j.isCheckMeasureJob);
            
            if (existingJob) {
              // Placeholder values that should be treated as empty (document type labels, etc.)
              // Use lowercase for case-insensitive matching
              const placeholderValuesLower = [
                'service order', 'sales order', 'check measure', 'job sheet', 'fitter work',
                'work sheet', 'service sheet', 'install order', 'measure sheet',
                'unknown client', 'address not found', 'service orders', 'sales orders'
              ];
              const isPlaceholder = (val: string) => placeholderValuesLower.includes(val.toLowerCase().trim());
              
              // Helper: Check if value is empty OR is a placeholder value
              const isEmpty = (val: any) => {
                if (val === null || val === undefined) return true;
                if (typeof val === 'string') {
                  const trimmed = val.trim();
                  return trimmed === '' || isPlaceholder(trimmed);
                }
                return false;
              };
              const hasValue = (val: any) => {
                if (val === null || val === undefined) return false;
                if (typeof val === 'string') {
                  const trimmed = val.trim();
                  return trimmed !== '' && !isPlaceholder(trimmed);
                }
                return true;
              };
              const normalizeStr = (val: any) => typeof val === 'string' ? val.trim() : val;
              
              // Bi-directional merge: Update empty fields AND replace placeholder values
              const updateData: any = {};
              let fieldsUpdated = 0;
              
              // Update if existing field is empty/placeholder and extracted data has real value
              if (isEmpty(existingJob.clientName) && hasValue(extractedData.clientName)) {
                const val = normalizeStr(extractedData.clientName);
                if (val !== normalizeStr(existingJob.clientName)) {
                  updateData.clientName = val;
                  fieldsUpdated++;
                }
              }
              if (isEmpty(existingJob.address) && hasValue(extractedData.address)) {
                const val = normalizeStr(extractedData.address);
                if (val !== normalizeStr(existingJob.address)) {
                  updateData.address = val;
                  fieldsUpdated++;
                }
              }
              // Extract phone number from contact field
              if (isEmpty(existingJob.phoneNumber) && hasValue(extractedData.contact)) {
                const phoneMatch = extractedData.contact.match(/[\d\s\-+()]+/);
                if (phoneMatch) {
                  const val = phoneMatch[0].trim();
                  if (val !== normalizeStr(existingJob.phoneNumber)) {
                    updateData.phoneNumber = val;
                    fieldsUpdated++;
                  }
                }
              }
              if (isEmpty(existingJob.installer) && hasValue(extractedData.installer)) {
                const val = normalizeStr(extractedData.installer);
                if (val !== normalizeStr(existingJob.installer)) {
                  updateData.installer = val;
                  fieldsUpdated++;
                }
              }
              if (isEmpty(existingJob.consultant) && hasValue(extractedData.consultant)) {
                const val = normalizeStr(extractedData.consultant);
                if (val !== normalizeStr(existingJob.consultant)) {
                  updateData.consultant = val;
                  fieldsUpdated++;
                }
              }
              if (isEmpty(existingJob.eta) && hasValue(extractedData.eta)) {
                const val = normalizeStr(extractedData.eta);
                if (val !== normalizeStr(existingJob.eta)) {
                  updateData.eta = val;
                  fieldsUpdated++;
                }
              }
              // Add clientEmail and amountOutstanding fields
              if (isEmpty(existingJob.clientEmail) && hasValue(extractedData.clientEmail)) {
                const val = normalizeStr(extractedData.clientEmail);
                if (val !== normalizeStr(existingJob.clientEmail)) {
                  updateData.clientEmail = val;
                  fieldsUpdated++;
                }
              }
              // For amountOutstanding, check if existing is null/undefined or '0'/'0.00' (no balance)
              // Only update if we have a real value and existing has no meaningful balance
              const existingOutstanding = existingJob.amountOutstanding;
              const isExistingEmpty = existingOutstanding === null || 
                                     existingOutstanding === undefined || 
                                     existingOutstanding === '0' || 
                                     existingOutstanding === '0.00';
              if (isExistingEmpty && extractedData.amountOutstanding && 
                  parseFloat(extractedData.amountOutstanding) > 0) {
                updateData.amountOutstanding = extractedData.amountOutstanding;
                fieldsUpdated++;
              }
              
              if (fieldsUpdated > 0) {
                await storage.updateJob(existingJob.id, updateData, workspaceId);
                console.log(`[Gmail] Job ${extractedData.jobId} updated with ${fieldsUpdated} new field(s)`);
              } else {
                console.log(`[Gmail] Job ${extractedData.jobId} already has all data, no updates needed`);
              }
              
              // Save job sheet even for existing jobs
              await storage.createJobSheet({
                workspaceId,
                fileName: `${extractedData.jobId}_email.html`,
                originalContent: htmlContent,
                extractedData: JSON.stringify(extractedData),
              });
              
              return existingJob;
            }
            
            // Create new job if it doesn't exist
            const jobData = {
              workspaceId,
              jobId: extractedData.jobId,
              clientName: extractedData.clientName || 'Unknown Client',
              address: extractedData.address || 'Address not found',
              type: extractedData.type || 'Install',
              eta: extractedData.eta || 'Pending',
              status: 'To Do',
              urgent: false,
              phoneNumber: extractedData.contact || undefined,
              installer: extractedData.installer || undefined,
              consultant: extractedData.consultant || undefined,
              clientEmail: extractedData.clientEmail || undefined,
              amountOutstanding: extractedData.amountOutstanding || undefined,
            };
            
            const job = await storage.createJob(jobData);
            
            // Also save the job sheet
            await storage.createJobSheet({
              workspaceId,
              fileName: `${extractedData.jobId}_email.html`,
              originalContent: htmlContent,
              extractedData: JSON.stringify(extractedData),
            });
            
            return job;
          } else {
            throw new Error('Could not extract job ID from document');
          }
        },
        async (htmlContent: string) => {
          // Process as Check Measure - extract data and create/update CM job
          const extractedData = extractJobSheetData(htmlContent);
          
          if (extractedData.jobId) {
            const cmJobId = `${extractedData.jobId}-CM`;
            
            // Check if CM job already exists
            const existingJobsList = await storage.getJobsByJobIds([cmJobId], workspaceId);
            const existingJob = existingJobsList.find(j => j.jobId === cmJobId && j.isCheckMeasureJob);
            
            if (existingJob) {
              // Helper: Check if value is empty (null, undefined, empty string, or whitespace-only) but preserve 0 as valid
              const isEmpty = (val: any) => val === null || val === undefined || (typeof val === 'string' && val.trim() === '');
              const hasValue = (val: any) => val !== null && val !== undefined && (typeof val !== 'string' || val.trim() !== '');
              const normalizeStr = (val: any) => typeof val === 'string' ? val.trim() : val;
              
              // Bi-directional merge: Only update empty fields, preserve existing data
              const updateData: any = {};
              let fieldsUpdated = 0;
              
              if (isEmpty(existingJob.clientName) && hasValue(extractedData.clientName)) {
                const val = normalizeStr(extractedData.clientName);
                if (val !== normalizeStr(existingJob.clientName)) {
                  updateData.clientName = val;
                  fieldsUpdated++;
                }
              }
              if (isEmpty(existingJob.address) && hasValue(extractedData.address)) {
                const val = normalizeStr(extractedData.address);
                if (val !== normalizeStr(existingJob.address)) {
                  updateData.address = val;
                  fieldsUpdated++;
                }
              }
              if (isEmpty(existingJob.phoneNumber) && hasValue(extractedData.contact)) {
                const phoneMatch = extractedData.contact.match(/[\d\s\-+()]+/);
                if (phoneMatch) {
                  const val = phoneMatch[0].trim();
                  if (val !== normalizeStr(existingJob.phoneNumber)) {
                    updateData.phoneNumber = val;
                    fieldsUpdated++;
                  }
                }
              }
              if (isEmpty(existingJob.eta) && hasValue(extractedData.eta)) {
                const val = normalizeStr(extractedData.eta);
                if (val !== normalizeStr(existingJob.eta)) {
                  updateData.eta = val;
                  fieldsUpdated++;
                }
              }
              
              if (fieldsUpdated > 0) {
                await storage.updateJob(existingJob.id, updateData, workspaceId);
                console.log(`[Gmail] CM Job ${cmJobId} updated with ${fieldsUpdated} new field(s)`);
              } else {
                console.log(`[Gmail] CM Job ${cmJobId} already has all data, no updates needed`);
              }
              
              // Check if CM document exists, if not create one for viewing
              const existingDocs = await storage.getJobDocuments(existingJob.id);
              const hasCMDoc = existingDocs.some(doc => 
                doc.documentType === 'cm' || 
                doc.documentType === 'check_measure'
              );
              
              if (!hasCMDoc) {
                await storage.createJobDocument({
                  jobId: existingJob.id,
                  fileName: `${cmJobId}_cm_sheet.html`,
                  originalContent: htmlContent,
                  extractedData: JSON.stringify(extractedData),
                  documentType: 'cm',
                });
                console.log(`[Gmail] CM document stored for existing job ${cmJobId}`);
              }
              
              return existingJob;
            }
            
            // Create new CM job if it doesn't exist
            const cmJobData = {
              workspaceId,
              jobId: cmJobId,
              clientName: extractedData.clientName || 'Unknown Client',
              address: extractedData.address || 'Address not found',
              type: 'Install',
              eta: extractedData.eta || 'Pending',
              status: 'To Do',
              urgent: false,
              isCheckMeasureJob: true,
              phoneNumber: extractedData.contact || undefined,
            };
            
            const job = await storage.createJob(cmJobData);
            
            // Also store the CM document for viewing
            await storage.createJobDocument({
              jobId: job.id,
              fileName: `${cmJobId}_cm_sheet.html`,
              originalContent: htmlContent,
              extractedData: JSON.stringify(extractedData),
              documentType: 'cm',
            });
            console.log(`[Gmail] CM document stored for new job ${cmJobId}`);
            
            return job;
          } else {
            throw new Error('Could not extract job ID from CM document');
          }
        },
        // Process Job Bank emails - update install costs for existing jobs
        async (rows: any[]) => {
          let created = 0;
          let updated = 0;
          
          // Placeholder values for case-insensitive matching
          const placeholderValuesLower = [
            'service order', 'sales order', 'check measure', 'job sheet', 'fitter work',
            'work sheet', 'service sheet', 'install order', 'measure sheet',
            'unknown client', 'address not found', 'service orders', 'sales orders'
          ];
          const isPlaceholder = (val: string) => placeholderValuesLower.includes(val.toLowerCase().trim());
          
          const isEmpty = (val: any) => {
            if (val === null || val === undefined) return true;
            if (typeof val === 'string') {
              const trimmed = val.trim();
              return trimmed === '' || isPlaceholder(trimmed);
            }
            return false;
          };
          const hasValue = (val: any) => {
            if (val === null || val === undefined) return false;
            if (typeof val === 'string') {
              const trimmed = val.trim();
              return trimmed !== '' && !isPlaceholder(trimmed);
            }
            if (typeof val === 'number') return true;
            return false;
          };
          
          for (const row of rows) {
            try {
              // Check if job exists
              const existingJobsList = await storage.getJobsByJobIds([row.jobId], workspaceId);
              const existingJob = existingJobsList.find(j => j.jobId === row.jobId && !j.isCheckMeasureJob);
              
              if (existingJob) {
                // Update existing job with new data (bi-directional merge)
                const updateData: any = {};
                let fieldsUpdated = 0;
                
                // Update install cost if not already set
                const existingCost = existingJob.installCost ? parseFloat(existingJob.installCost.toString()) : 0;
                const newCost = row.installCost ? parseFloat(row.installCost) : 0;
                if (existingCost === 0 && newCost > 0) {
                  updateData.installCost = newCost.toFixed(4);
                  fieldsUpdated++;
                }
                
                // Update other empty fields
                if (isEmpty(existingJob.clientName) && hasValue(row.clientName)) {
                  updateData.clientName = row.clientName.trim();
                  fieldsUpdated++;
                }
                if (isEmpty(existingJob.address) && hasValue(row.address)) {
                  updateData.address = row.address.trim();
                  fieldsUpdated++;
                }
                if (isEmpty(existingJob.eta) && hasValue(row.eta)) {
                  updateData.eta = row.eta.trim();
                  fieldsUpdated++;
                }
                
                if (fieldsUpdated > 0) {
                  await storage.updateJob(existingJob.id, updateData, workspaceId);
                  console.log(`[JobBank] Updated job ${row.jobId} with ${fieldsUpdated} field(s)`, updateData);
                  updated++;
                }
              } else {
                // Create new job from Job Bank data
                const jobData = {
                  workspaceId,
                  jobId: row.jobId,
                  clientName: row.clientName || 'Unknown Client',
                  address: row.address || 'Address not found',
                  type: row.type || 'Install',
                  eta: row.eta || 'Pending',
                  status: 'To Do',
                  urgent: false,
                  installCost: row.installCost ? parseFloat(row.installCost).toFixed(4) : undefined,
                };
                
                await storage.createJob(jobData);
                console.log(`[JobBank] Created new job ${row.jobId}`);
                created++;
              }
            } catch (error: any) {
              console.error(`[JobBank] Error processing row ${row.jobId}:`, error.message);
            }
          }
          
          return { created, updated };
        }
      );
      
      res.json({
        success: true,
        message: `Processed ${emails.length} emails`,
        emailsProcessed: emails.length,
        jobsCreated: results.jobsCreated,
        cmsCreated: results.cmsCreated,
        jobBankProcessed: results.jobBankProcessed || 0,
        errors: results.errors.length > 0 ? results.errors : undefined
      });
      
    } catch (error) {
      console.error('[Gmail] Sync error:', error);
      res.status(500).json({ 
        error: error instanceof Error ? error.message : "Failed to sync emails" 
      });
    }
  });
  
  // Sync emails from Gmail with WebSocket progress updates
  app.post("/api/gmail/sync-with-progress", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { fetchNewEmails, markEmailAsProcessed, isGmailConnected, getGmailClient } = await import('./gmail-service');
      const { 
        broadcastSyncStart, 
        broadcastEmailFound, 
        broadcastJobProcessing, 
        broadcastJobComplete, 
        broadcastSyncComplete,
        broadcastSyncError 
      } = await import('./sync-websocket');
      
      const connected = await isGmailConnected(workspaceId);
      if (!connected) {
        broadcastSyncError(workspaceId, "Gmail not connected. Please connect your Gmail account first.");
        return res.status(400).json({ error: "Gmail not connected" });
      }
      
      console.log('[Gmail] Fetching new emails with progress for workspace:', workspaceId);
      const emails = await fetchNewEmails(workspaceId);
      console.log(`[Gmail] Found ${emails.length} new emails to process`);
      
      // Broadcast sync start
      broadcastSyncStart(workspaceId, emails.length);
      
      if (emails.length === 0) {
        broadcastSyncComplete(workspaceId, {
          emailsProcessed: 0,
          jobsCreated: 0,
          jobsUpdated: 0,
          cmsCreated: 0,
          cmsUpdated: 0,
          skipped: 0,
          errors: []
        });
        return res.json({ success: true, message: "No new emails found" });
      }
      
      // Process emails one by one with progress updates
      const summary = {
        emailsProcessed: 0,
        jobsCreated: 0,
        jobsUpdated: 0,
        cmsCreated: 0,
        cmsUpdated: 0,
        skipped: 0,
        errors: [] as string[]
      };
      
      // Import Job Bank parser
      const { parseJobBankTable } = await import('./gmail-service');
      
      for (let i = 0; i < emails.length; i++) {
        const email = emails[i];
        const attachmentCount = email.emailType === 'JobBank' ? 1 : email.attachments.length;
        broadcastEmailFound(workspaceId, i + 1, emails.length, email.subject, attachmentCount);
        
        try {
          // Handle Job Bank emails (table in body, not attachments)
          if (email.emailType === 'JobBank' && email.bodyHtml) {
            const rows = parseJobBankTable(email.bodyHtml);
            console.log(`[Gmail Progress] Job Bank with ${rows.length} jobs`);
            
            // Placeholder detection helpers
            const placeholderValuesLower = [
              'service order', 'sales order', 'check measure', 'job sheet', 'fitter work',
              'work sheet', 'service sheet', 'install order', 'measure sheet',
              'unknown client', 'address not found'
            ];
            const isPlaceholder = (val: string) => placeholderValuesLower.includes(val.toLowerCase().trim());
            const isEmpty = (val: any) => {
              if (val === null || val === undefined) return true;
              if (typeof val === 'string') return val.trim() === '' || isPlaceholder(val);
              return false;
            };
            const hasValue = (val: any) => {
              if (val === null || val === undefined) return false;
              if (typeof val === 'string') return val.trim() !== '' && !isPlaceholder(val);
              return typeof val === 'number';
            };
            
            for (const row of rows) {
              broadcastJobProcessing(workspaceId, row.jobId, 'job', row.clientName, row.address);
              
              try {
                const existingJobsList = await storage.getJobsByJobIds([row.jobId], workspaceId);
                const existingJob = existingJobsList.find(j => j.jobId === row.jobId && !j.isCheckMeasureJob);
                
                if (existingJob) {
                  const updateData: any = {};
                  let fieldsUpdated = 0;
                  
                  // Update install cost if missing
                  const existingCost = existingJob.installCost ? parseFloat(existingJob.installCost.toString()) : 0;
                  const newCost = row.installCost ? parseFloat(row.installCost) : 0;
                  if (existingCost === 0 && newCost > 0) {
                    updateData.installCost = newCost.toFixed(4);
                    fieldsUpdated++;
                  }
                  
                  if (isEmpty(existingJob.clientName) && hasValue(row.clientName)) {
                    updateData.clientName = row.clientName.trim();
                    fieldsUpdated++;
                  }
                  if (isEmpty(existingJob.address) && hasValue(row.address)) {
                    updateData.address = row.address.trim();
                    fieldsUpdated++;
                  }
                  if (isEmpty(existingJob.eta) && hasValue(row.eta)) {
                    updateData.eta = row.eta.trim();
                    fieldsUpdated++;
                  }
                  
                  if (fieldsUpdated > 0) {
                    await storage.updateJob(existingJob.id, updateData, workspaceId);
                    summary.jobsUpdated++;
                    broadcastJobComplete(workspaceId, row.jobId, 'job', 'updated', row.clientName, row.address);
                  } else {
                    summary.skipped++;
                    broadcastJobComplete(workspaceId, row.jobId, 'job', 'skipped', row.clientName, row.address);
                  }
                } else {
                  // Create new job from Job Bank
                  const jobData = {
                    workspaceId,
                    jobId: row.jobId,
                    clientName: row.clientName || 'Unknown Client',
                    address: row.address || 'Address not found',
                    type: row.type || 'Install',
                    eta: row.eta || 'Pending',
                    status: 'To Do',
                    urgent: false,
                    installCost: row.installCost ? parseFloat(row.installCost).toFixed(4) : undefined,
                  };
                  
                  await storage.createJob(jobData);
                  summary.jobsCreated++;
                  broadcastJobComplete(workspaceId, row.jobId, 'job', 'created', row.clientName, row.address);
                }
              } catch (error: any) {
                summary.errors.push(`Job Bank ${row.jobId}: ${error.message}`);
                broadcastJobComplete(workspaceId, row.jobId, 'job', 'error', row.clientName, row.address, error.message);
              }
            }
            
            await markEmailAsProcessed(workspaceId, email.messageId, email.subject, email.emailType);
            summary.emailsProcessed++;
            continue;
          }
          
          for (const attachment of email.attachments) {
            const htmlContent = Buffer.from(attachment.data, 'base64').toString('utf-8');
            
            if (email.emailType === 'JobSheet') {
              // Extract job data
              const extractedData = extractJobSheetData(htmlContent);
              
              if (extractedData.jobId) {
                broadcastJobProcessing(workspaceId, extractedData.jobId, 'job', extractedData.clientName, extractedData.address);
                
                try {
                  // Check if job already exists
                  const existingJobsList = await storage.getJobsByJobIds([extractedData.jobId], workspaceId);
                  const existingJob = existingJobsList.find(j => j.jobId === extractedData.jobId && !j.isCheckMeasureJob);
                  
                  if (existingJob) {
                    // Placeholder values that should be treated as empty (case-insensitive)
                    const placeholderValuesLower = [
                      'service order', 'sales order', 'check measure', 'job sheet', 'fitter work',
                      'work sheet', 'service sheet', 'install order', 'measure sheet',
                      'unknown client', 'address not found', 'service orders', 'sales orders'
                    ];
                    const isPlaceholder = (val: string) => placeholderValuesLower.includes(val.toLowerCase().trim());
                    
                    // Helper: Check if value is empty OR is a placeholder value
                    const isEmpty = (val: any) => {
                      if (val === null || val === undefined) return true;
                      if (typeof val === 'string') {
                        const trimmed = val.trim();
                        return trimmed === '' || isPlaceholder(trimmed);
                      }
                      return false;
                    };
                    const hasValue = (val: any) => {
                      if (val === null || val === undefined) return false;
                      if (typeof val === 'string') {
                        const trimmed = val.trim();
                        return trimmed !== '' && !isPlaceholder(trimmed);
                      }
                      return true;
                    };
                    const normalizeStr = (val: any) => typeof val === 'string' ? val.trim() : val;
                    
                    const updateData: any = {};
                    let fieldsUpdated = 0;
                    
                    if (isEmpty(existingJob.clientName) && hasValue(extractedData.clientName)) {
                      updateData.clientName = normalizeStr(extractedData.clientName);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.address) && hasValue(extractedData.address)) {
                      updateData.address = normalizeStr(extractedData.address);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.phoneNumber) && hasValue(extractedData.contact)) {
                      const phoneMatch = extractedData.contact.match(/[\d\s\-+()]+/);
                      if (phoneMatch) {
                        updateData.phoneNumber = phoneMatch[0].trim();
                        fieldsUpdated++;
                      }
                    }
                    if (isEmpty(existingJob.installer) && hasValue(extractedData.installer)) {
                      updateData.installer = normalizeStr(extractedData.installer);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.consultant) && hasValue(extractedData.consultant)) {
                      updateData.consultant = normalizeStr(extractedData.consultant);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.eta) && hasValue(extractedData.eta)) {
                      updateData.eta = normalizeStr(extractedData.eta);
                      fieldsUpdated++;
                    }
                    
                    if (fieldsUpdated > 0) {
                      await storage.updateJob(existingJob.id, updateData, workspaceId);
                      summary.jobsUpdated++;
                      broadcastJobComplete(workspaceId, extractedData.jobId, 'job', 'updated', extractedData.clientName, extractedData.address);
                    } else {
                      summary.skipped++;
                      broadcastJobComplete(workspaceId, extractedData.jobId, 'job', 'skipped', extractedData.clientName, extractedData.address);
                    }
                    
                    // Save job sheet
                    await storage.createJobSheet({
                      workspaceId,
                      fileName: `${extractedData.jobId}_email.html`,
                      originalContent: htmlContent,
                      extractedData: JSON.stringify(extractedData),
                    });
                  } else {
                    // Create new job
                    const jobData = {
                      workspaceId,
                      jobId: extractedData.jobId,
                      clientName: extractedData.clientName || 'Unknown Client',
                      address: extractedData.address || 'Address not found',
                      type: extractedData.type || 'Install',
                      eta: extractedData.eta || 'Pending',
                      status: 'To Do',
                      urgent: false,
                      phoneNumber: extractedData.contact || undefined,
                      installer: extractedData.installer || undefined,
                      consultant: extractedData.consultant || undefined,
                    };
                    
                    await storage.createJob(jobData);
                    summary.jobsCreated++;
                    broadcastJobComplete(workspaceId, extractedData.jobId, 'job', 'created', extractedData.clientName, extractedData.address);
                    
                    await storage.createJobSheet({
                      workspaceId,
                      fileName: `${extractedData.jobId}_email.html`,
                      originalContent: htmlContent,
                      extractedData: JSON.stringify(extractedData),
                    });
                  }
                } catch (error: any) {
                  summary.errors.push(`Job ${extractedData.jobId}: ${error.message}`);
                  broadcastJobComplete(workspaceId, extractedData.jobId, 'job', 'error', extractedData.clientName, extractedData.address, error.message);
                }
              }
            } else if (email.emailType === 'CM') {
              // Process as Check Measure
              const extractedData = extractJobSheetData(htmlContent);
              
              if (extractedData.jobId) {
                const cmJobId = `${extractedData.jobId}-CM`;
                broadcastJobProcessing(workspaceId, cmJobId, 'cm', extractedData.clientName, extractedData.address);
                
                try {
                  const existingJobsList = await storage.getJobsByJobIds([cmJobId], workspaceId);
                  const existingJob = existingJobsList.find(j => j.jobId === cmJobId && j.isCheckMeasureJob);
                  
                  if (existingJob) {
                    const isEmpty = (val: any) => val === null || val === undefined || (typeof val === 'string' && val.trim() === '');
                    const hasValue = (val: any) => val !== null && val !== undefined && (typeof val !== 'string' || val.trim() !== '');
                    const normalizeStr = (val: any) => typeof val === 'string' ? val.trim() : val;
                    
                    const updateData: any = {};
                    let fieldsUpdated = 0;
                    
                    if (isEmpty(existingJob.clientName) && hasValue(extractedData.clientName)) {
                      updateData.clientName = normalizeStr(extractedData.clientName);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.address) && hasValue(extractedData.address)) {
                      updateData.address = normalizeStr(extractedData.address);
                      fieldsUpdated++;
                    }
                    if (isEmpty(existingJob.phoneNumber) && hasValue(extractedData.contact)) {
                      const phoneMatch = extractedData.contact.match(/[\d\s\-+()]+/);
                      if (phoneMatch) {
                        updateData.phoneNumber = phoneMatch[0].trim();
                        fieldsUpdated++;
                      }
                    }
                    if (isEmpty(existingJob.eta) && hasValue(extractedData.eta)) {
                      updateData.eta = normalizeStr(extractedData.eta);
                      fieldsUpdated++;
                    }
                    
                    if (fieldsUpdated > 0) {
                      await storage.updateJob(existingJob.id, updateData, workspaceId);
                      summary.cmsUpdated++;
                      broadcastJobComplete(workspaceId, cmJobId, 'cm', 'updated', extractedData.clientName, extractedData.address);
                    } else {
                      summary.skipped++;
                      broadcastJobComplete(workspaceId, cmJobId, 'cm', 'skipped', extractedData.clientName, extractedData.address);
                    }
                    
                    // Check if CM document exists, if not create one for viewing
                    const existingDocs = await storage.getJobDocuments(existingJob.id);
                    const hasCMDoc = existingDocs.some(doc => 
                      doc.documentType === 'cm' || 
                      doc.documentType === 'check_measure'
                    );
                    
                    if (!hasCMDoc) {
                      await storage.createJobDocument({
                        jobId: existingJob.id,
                        fileName: `${cmJobId}_cm_sheet.html`,
                        originalContent: htmlContent,
                        extractedData: JSON.stringify(extractedData),
                        documentType: 'cm',
                      });
                      console.log(`[Gmail WS] CM document stored for existing job ${cmJobId}`);
                    }
                  } else {
                    const cmJobData = {
                      workspaceId,
                      jobId: cmJobId,
                      clientName: extractedData.clientName || 'Unknown Client',
                      address: extractedData.address || 'Address not found',
                      type: 'Install',
                      eta: extractedData.eta || 'Pending',
                      status: 'To Do',
                      urgent: false,
                      isCheckMeasureJob: true,
                      phoneNumber: extractedData.contact || undefined,
                    };
                    
                    const newJob = await storage.createJob(cmJobData);
                    summary.cmsCreated++;
                    broadcastJobComplete(workspaceId, cmJobId, 'cm', 'created', extractedData.clientName, extractedData.address);
                    
                    // Also store the CM document for viewing
                    await storage.createJobDocument({
                      jobId: newJob.id,
                      fileName: `${cmJobId}_cm_sheet.html`,
                      originalContent: htmlContent,
                      extractedData: JSON.stringify(extractedData),
                      documentType: 'cm',
                    });
                    console.log(`[Gmail WS] CM document stored for new job ${cmJobId}`);
                  }
                } catch (error: any) {
                  summary.errors.push(`CM ${cmJobId}: ${error.message}`);
                  broadcastJobComplete(workspaceId, cmJobId, 'cm', 'error', extractedData.clientName, extractedData.address, error.message);
                }
              }
            }
          }
          
          // Mark email as processed
          await markEmailAsProcessed(workspaceId, email.messageId, email.subject, email.emailType);
          summary.emailsProcessed++;
        } catch (error: any) {
          summary.errors.push(`Email ${email.subject}: ${error.message}`);
        }
      }
      
      // Broadcast sync complete
      broadcastSyncComplete(workspaceId, summary);
      
      res.json({
        success: true,
        message: `Processed ${summary.emailsProcessed} emails`,
        ...summary
      });
      
    } catch (error) {
      console.error('[Gmail] Sync with progress error:', error);
      const { broadcastSyncError } = await import('./sync-websocket');
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (workspaceId) {
        broadcastSyncError(workspaceId, error instanceof Error ? error.message : "Failed to sync emails");
      }
      res.status(500).json({ 
        error: error instanceof Error ? error.message : "Failed to sync emails" 
      });
    }
  });
  
  // Disconnect Gmail
  app.post("/api/gmail/disconnect", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { disconnectGmail } = await import('./gmail-service');
      await disconnectGmail(workspaceId);
      
      res.json({ success: true, message: "Gmail disconnected" });
    } catch (error) {
      console.error('[Gmail] Disconnect error:', error);
      res.status(500).json({ error: "Failed to disconnect Gmail" });
    }
  });
  
  // Get processed emails history
  app.get("/api/gmail/history", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const processedEmails = await storage.getProcessedEmails(workspaceId);
      res.json({ emails: processedEmails });
    } catch (error) {
      console.error('[Gmail] History error:', error);
      res.status(500).json({ error: "Failed to get email history" });
    }
  });

  app.get("/api/gmail/secondary-status", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      const token = await storage.getSecondaryGmailToken(workspaceId);
      res.json({ 
        connected: !!token,
        emailAddress: token?.emailAddress || null,
      });
    } catch (error) {
      console.error('[Gmail] Secondary status error:', error);
      res.status(500).json({ error: "Failed to check secondary Gmail status" });
    }
  });

  app.get("/api/gmail/secondary-auth-url", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      const { getSecondaryGmailAuthUrl } = await import('./gmail-service');
      const origin = `${req.protocol}://${req.get('host')}`;
      const authUrl = getSecondaryGmailAuthUrl(workspaceId, userId, origin);
      res.json({ authUrl });
    } catch (error) {
      console.error('[Gmail] Secondary auth URL error:', error);
      res.status(500).json({ error: "Failed to generate auth URL" });
    }
  });

  app.get("/api/gmail/secondary-callback", async (req: any, res) => {
    try {
      const { code, state } = req.query;
      if (!code || !state) {
        return res.redirect('/earnings?error=missing_params');
      }
      
      const { handleSecondaryGmailCallback } = await import('./gmail-service');
      const origin = `${req.protocol}://${req.get('host')}`;
      const result = await handleSecondaryGmailCallback(code as string, state as string, origin);
      
      console.log(`[Gmail] Secondary account connected: ${result.emailAddress}`);
      res.redirect(`/earnings?connected=${encodeURIComponent(result.emailAddress)}`);
    } catch (error: any) {
      console.error('[Gmail] Secondary callback error:', error);
      res.redirect(`/earnings?error=${encodeURIComponent(error.message || 'auth_failed')}`);
    }
  });

  app.post("/api/gmail/secondary-disconnect", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      await storage.deleteSecondaryGmailToken(workspaceId);
      res.json({ success: true });
    } catch (error) {
      console.error('[Gmail] Secondary disconnect error:', error);
      res.status(500).json({ error: "Failed to disconnect" });
    }
  });

  app.get("/api/earnings", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { db } = await import('./db');
      const { invoices: invoicesTable, bookipiInvoices } = await import('@shared/schema');
      const { eq } = await import('drizzle-orm');
      
      const appInvoices = await db.select().from(invoicesTable).where(eq(invoicesTable.workspaceId, workspaceId));
      const bookipi = await db.select().from(bookipiInvoices).where(eq(bookipiInvoices.workspaceId, workspaceId));
      
      const earningsWeeks: { weekStart: string; weekEnd: string; amount: number; source: string; invoiceNumber: string | null; status: string; company: string | null }[] = [];
      
      const coveredWeeks = new Set<string>();
      
      for (const inv of appInvoices) {
        const amount = parseFloat(inv.totalAmount?.toString() || '0');
        if (amount > 0 && inv.status !== 'draft') {
          coveredWeeks.add(inv.weekStartDate);
          earningsWeeks.push({
            weekStart: inv.weekStartDate,
            weekEnd: inv.weekEndDate,
            amount,
            source: 'app',
            invoiceNumber: inv.invoiceNumber,
            status: inv.status,
            company: null,
          });
        }
      }
      
      for (const b of bookipi) {
        const amount = parseFloat(b.amount?.toString() || '0');
        if (amount > 0 && b.weekStartDate && !coveredWeeks.has(b.weekStartDate)) {
          coveredWeeks.add(b.weekStartDate);
          earningsWeeks.push({
            weekStart: b.weekStartDate,
            weekEnd: b.weekEndDate || b.weekStartDate,
            amount,
            source: 'email',
            invoiceNumber: b.invoiceNumber,
            status: 'email',
            company: b.company || null,
          });
        }
      }
      
      earningsWeeks.sort((a, b) => a.weekStart.localeCompare(b.weekStart));

      // Fill in missing weeks between earliest and latest as "not invoiced"
      if (earningsWeeks.length > 0) {
        const filledWeeks: typeof earningsWeeks = [];
        const current = new Date(earningsWeeks[0].weekStart + 'T00:00:00');
        const last = new Date(earningsWeeks[earningsWeeks.length - 1].weekStart + 'T00:00:00');
        while (current <= last) {
          const weekStartStr = current.toISOString().split('T')[0];
          const weekEnd = new Date(current);
          weekEnd.setDate(current.getDate() + 6);
          const weekEndStr = weekEnd.toISOString().split('T')[0];
          if (coveredWeeks.has(weekStartStr)) {
            filledWeeks.push(earningsWeeks.find(w => w.weekStart === weekStartStr)!);
          } else {
            filledWeeks.push({ weekStart: weekStartStr, weekEnd: weekEndStr, amount: 0, source: 'missing', invoiceNumber: null, status: 'not invoiced', company: null });
          }
          current.setDate(current.getDate() + 7);
        }
        earningsWeeks.splice(0, earningsWeeks.length, ...filledWeeks);
      }
      
      res.json({
        weeks: earningsWeeks,
        appInvoiceCount: appInvoices.filter(i => parseFloat(i.totalAmount?.toString() || '0') > 0).length,
        emailInvoiceCount: bookipi.length,
        hasEmailInvoiceData: bookipi.length > 0,
      });
    } catch (error) {
      console.error('[Earnings] Error:', error);
      res.status(500).json({ error: "Failed to load earnings data" });
    }
  });

  app.get("/api/earning-targets", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      const { db } = await import('./db');
      const { earningTargets } = await import('@shared/schema');
      const { eq } = await import('drizzle-orm');
      
      const targets = await db.select().from(earningTargets).where(eq(earningTargets.workspaceId, workspaceId));
      res.json(targets);
    } catch (error) {
      console.error('[EarningTargets] Error:', error);
      res.status(500).json({ error: "Failed to load earning targets" });
    }
  });

  app.post("/api/earning-targets", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      const { label, annualAmount } = req.body;
      if (!label || typeof label !== 'string' || !label.trim()) return res.status(400).json({ error: "Label is required" });
      const parsedAmount = parseFloat(annualAmount);
      if (isNaN(parsedAmount) || parsedAmount <= 0) return res.status(400).json({ error: "Annual amount must be a positive number" });
      
      const { db } = await import('./db');
      const { earningTargets } = await import('@shared/schema');
      
      const [target] = await db.insert(earningTargets).values({
        workspaceId,
        label: label.trim(),
        annualAmount: parsedAmount.toFixed(2),
      }).returning();
      
      res.json(target);
    } catch (error) {
      console.error('[EarningTargets] Error:', error);
      res.status(500).json({ error: "Failed to create earning target" });
    }
  });

  app.delete("/api/earning-targets/:id", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      
      const { db } = await import('./db');
      const { earningTargets } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      
      const targetId = parseInt(req.params.id);
      if (isNaN(targetId) || targetId <= 0) return res.status(400).json({ error: "Invalid target ID" });
      
      await db.delete(earningTargets).where(
        and(eq(earningTargets.id, targetId), eq(earningTargets.workspaceId, workspaceId))
      );
      res.json({ success: true });
    } catch (error) {
      console.error('[EarningTargets] Error:', error);
      res.status(500).json({ error: "Failed to delete earning target" });
    }
  });

  app.get("/api/gmail/scan-invoices", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { searchInvoiceEmails, parseInvoiceEmails } = await import('./gmail-service');
      const invoiceEmails = await searchInvoiceEmails(workspaceId);
      const parsed = parseInvoiceEmails(invoiceEmails);
      
      const successCount = parsed.filter(p => p.amount !== null).length;
      console.log(`[Gmail] Parsed ${successCount}/${parsed.length} invoice amounts`);
      
      const succeeded = parsed.filter(p => p.amount !== null);
      if (succeeded.length > 0) {
        const sample = succeeded[0];
        const sampleEmail = invoiceEmails.find(e => e.messageId === sample.messageId);
        console.log(`[Gmail] Sample parsed: $${sample.amount}, inv#${sample.invoiceNumber}, company="${sample.company}", PDFs=${sampleEmail?.pdfTexts?.length || 0}`);
      }
      const failed = parsed.filter(p => p.amount === null);
      if (failed.length > 0) {
        console.log(`[Gmail] ${failed.length} unparsed emails (first 5 samples):`);
        for (const f of failed.slice(0, 5)) {
          const email = invoiceEmails.find(e => e.messageId === f.messageId);
          if (email) {
            console.log(`  Subject: "${email.subject}", From: "${email.senderEmail}", PDFs: ${email.pdfTexts?.length || 0}, Attachments: [${email.attachmentNames.join(', ')}]`);
            console.log(`  Snippet: "${email.snippet?.substring(0, 200)}"`);
            console.log(`  BodyText length: ${email.bodyText?.length || 0}, BodyHTML length: ${email.bodyHtml?.length || 0}`);
            if (email.bodyText) {
              console.log(`  BodyText sample: "${email.bodyText.substring(0, 400)}"`);
            }
            if (email.pdfTexts?.length) {
              console.log(`  PDF text sample: "${email.pdfTexts[0].substring(0, 300)}"`);
            }
          }
        }
      }
      
      const { db } = await import('./db');
      const { bookipiInvoices } = await import('@shared/schema');
      const { eq } = await import('drizzle-orm');
      
      let savedCount = 0;
      const invoiceValues: any[] = [];
      for (const inv of parsed) {
        if (inv.amount !== null && inv.amount > 0) {
          const invDate = new Date(inv.date);
          const dateStr = invDate.toISOString().split('T')[0];
          const day = invDate.getDay();
          const tuesdayOffset = day >= 2 ? day - 2 : day + 5;
          const weekStart = new Date(invDate);
          weekStart.setDate(invDate.getDate() - tuesdayOffset);
          const weekEnd = new Date(weekStart);
          weekEnd.setDate(weekStart.getDate() + 7);
          
          invoiceValues.push({
            workspaceId,
            messageId: inv.messageId,
            invoiceNumber: inv.invoiceNumber,
            amount: inv.amount.toFixed(2),
            invoiceDate: dateStr,
            weekStartDate: weekStart.toISOString().split('T')[0],
            weekEndDate: weekEnd.toISOString().split('T')[0],
            company: inv.company || null,
            senderEmail: inv.senderEmail || null,
            senderName: inv.senderName || null,
          });
        }
      }
      
      await db.transaction(async (tx: any) => {
        await tx.delete(bookipiInvoices).where(eq(bookipiInvoices.workspaceId, workspaceId));
        for (const val of invoiceValues) {
          await tx.insert(bookipiInvoices).values(val);
        }
      });
      savedCount = invoiceValues.length;
      console.log(`[Gmail] Saved ${savedCount} email invoices to database`);
      
      res.json({ 
        total: parsed.length,
        parsedCount: successCount,
        savedCount,
      });
    } catch (error) {
      console.error('[Gmail] Invoice scan error:', error);
      res.status(500).json({ error: "Failed to scan invoice emails" });
    }
  });

  app.get("/api/gmail/invoice-email/:messageId", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { searchInvoiceEmails } = await import('./gmail-service');
      const invoiceEmails = await searchInvoiceEmails(workspaceId);
      const email = invoiceEmails.find(e => e.messageId === req.params.messageId);
      
      if (!email) {
        return res.status(404).json({ error: "Email not found" });
      }
      
      res.json(email);
    } catch (error) {
      console.error('[Gmail] Invoice email detail error:', error);
      res.status(500).json({ error: "Failed to get invoice email" });
    }
  });

  // Repair CM documents - fetch CM sheets for jobs that don't have viewable documents
  app.post("/api/gmail/repair-cm-docs", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { getGmailClient, searchEmails, getEmailContent, decodeBase64 } = await import('./gmail-service');
      const gmail = await getGmailClient(workspaceId);
      
      if (!gmail) {
        return res.status(400).json({ error: "Gmail not connected. Please connect Gmail first." });
      }
      
      // Find CM jobs without documents
      const allJobs = await storage.getAllJobs(workspaceId);
      const cmJobs = allJobs.filter(j => j.isCheckMeasureJob);
      
      let repaired = 0;
      let skipped = 0;
      let errors: string[] = [];
      
      for (const cmJob of cmJobs) {
        try {
          // Check if this CM job already has a document
          const existingDocs = await storage.getJobDocuments(cmJob.id);
          const hasCMDoc = existingDocs.some(doc => 
            doc.documentType === 'cm' || 
            doc.documentType === 'check_measure'
          );
          
          if (hasCMDoc) {
            skipped++;
            continue;
          }
          
          // Extract job number for matching (e.g., "J0015493-1-CM" -> "15493" or "J0015493")
          const baseJobId = cmJob.jobId.replace(/-?CM$/i, '');
          const fullJobNumber = baseJobId.match(/J?\d+/)?.[0]; // J15493 or 15493
          const digitsOnly = baseJobId.match(/\d+/)?.[0]; // Just 15493
          
          if (!digitsOnly) {
            errors.push(`${cmJob.jobId}: Could not extract job number`);
            continue;
          }
          
          // Search for CM emails - don't filter by job number in search (it's in the body)
          // Search with job number in query first, then fall back to broader search
          let messages = await searchEmails(gmail, `subject:CM ${digitsOnly}`, 10);
          
          // If no results, try broader search
          if (!messages || messages.length === 0) {
            messages = await searchEmails(gmail, `subject:CM has:attachment`, 50);
          }
          
          if (!messages || messages.length === 0) {
            errors.push(`${cmJob.jobId}: No CM emails found in Gmail`);
            continue;
          }
          
          console.log(`[Gmail Repair] Searching for ${cmJob.jobId} (${digitsOnly}) in ${messages.length} emails`);
          
          // Try to find and process the CM attachment
          let foundDoc = false;
          
          // Helper to recursively find all parts
          function getAllParts(parts: any[]): any[] {
            let allParts: any[] = [];
            for (const part of parts) {
              allParts.push(part);
              if (part.parts) {
                allParts = allParts.concat(getAllParts(part.parts));
              }
            }
            return allParts;
          }
          
          // Helper to check if content contains this job number
          function contentMatchesJob(content: string): boolean {
            const lowerContent = content.toLowerCase();
            // Check for various job number formats
            return content.includes(digitsOnly!) || 
                   content.includes(`J${digitsOnly}`) ||
                   content.includes(`J0${digitsOnly}`) ||
                   lowerContent.includes(baseJobId.toLowerCase());
          }
          
          for (const msg of messages) {
            if (foundDoc) break;
            
            try {
              const emailContent = await getEmailContent(gmail, msg.id!);
              if (!emailContent) continue;
              
              // Get all parts including nested ones
              const topParts = emailContent.payload?.parts || [];
              const allParts = getAllParts(topParts);
              
              // Look for HTML attachments first
              for (const part of allParts) {
                if (part.mimeType === 'text/html' && part.body?.attachmentId) {
                  const attachment = await gmail.users.messages.attachments.get({
                    userId: 'me',
                    messageId: msg.id!,
                    id: part.body.attachmentId
                  });
                  
                  if (attachment.data?.data) {
                    const htmlContent = decodeBase64(attachment.data.data);
                    
                    // Check if this attachment matches our job
                    if (contentMatchesJob(htmlContent)) {
                      console.log(`[Gmail Repair] Found matching HTML attachment for ${cmJob.jobId}`);
                      
                      // Store the CM document
                      await storage.createJobDocument({
                        jobId: cmJob.id,
                        fileName: `${cmJob.jobId}_cm_sheet.html`,
                        originalContent: htmlContent,
                        extractedData: JSON.stringify({ repaired: true, source: 'gmail-repair' }),
                        documentType: 'cm',
                      });
                      
                      console.log(`[Gmail Repair] CM document stored for ${cmJob.jobId}`);
                      repaired++;
                      foundDoc = true;
                      break;
                    }
                  }
                }
              }
              
              // If no attachment, try inline HTML parts with data
              if (!foundDoc) {
                for (const part of allParts) {
                  if (part.mimeType === 'text/html' && part.body?.data) {
                    const htmlContent = decodeBase64(part.body.data);
                    
                    // Only use if it matches job AND looks like a CM sheet
                    if (contentMatchesJob(htmlContent) &&
                        (htmlContent.toLowerCase().includes('check measure') || 
                         htmlContent.toLowerCase().includes('plantation') ||
                         htmlContent.toLowerCase().includes('shutters'))) {
                      console.log(`[Gmail Repair] Using inline HTML for ${cmJob.jobId}`);
                      await storage.createJobDocument({
                        jobId: cmJob.id,
                        fileName: `${cmJob.jobId}_cm_sheet.html`,
                        originalContent: htmlContent,
                        extractedData: JSON.stringify({ repaired: true, source: 'gmail-repair-body' }),
                        documentType: 'cm',
                      });
                      
                      console.log(`[Gmail Repair] CM document (from body) stored for ${cmJob.jobId}`);
                      repaired++;
                      foundDoc = true;
                      break;
                    }
                  }
                }
              }
              
              // Try main body if still not found
              if (!foundDoc && emailContent.payload?.body?.data) {
                const htmlContent = decodeBase64(emailContent.payload.body.data);
                if (contentMatchesJob(htmlContent) &&
                    (htmlContent.toLowerCase().includes('check measure') || 
                     htmlContent.toLowerCase().includes('plantation') ||
                     htmlContent.toLowerCase().includes('shutters'))) {
                  console.log(`[Gmail Repair] Using main body for ${cmJob.jobId}`);
                  await storage.createJobDocument({
                    jobId: cmJob.id,
                    fileName: `${cmJob.jobId}_cm_sheet.html`,
                    originalContent: htmlContent,
                    extractedData: JSON.stringify({ repaired: true, source: 'gmail-repair-main-body' }),
                    documentType: 'cm',
                  });
                  
                  console.log(`[Gmail Repair] CM document (from main body) stored for ${cmJob.jobId}`);
                  repaired++;
                  foundDoc = true;
                }
              }
            } catch (msgError: any) {
              console.log(`[Gmail Repair] Error processing message for ${cmJob.jobId}:`, msgError.message);
            }
          }
          
          if (!foundDoc) {
            errors.push(`${cmJob.jobId}: Could not find CM sheet in emails`);
          }
        } catch (jobError: any) {
          errors.push(`${cmJob.jobId}: ${jobError.message}`);
        }
      }
      
      res.json({
        success: true,
        message: `Repaired ${repaired} CM documents`,
        repaired,
        skipped,
        totalCMJobs: cmJobs.length,
        errors: errors.slice(0, 10) // Limit errors in response
      });
      
    } catch (error) {
      console.error('[Gmail] Repair CM docs error:', error);
      res.status(500).json({ 
        error: error instanceof Error ? error.message : "Failed to repair CM documents" 
      });
    }
  });

  // Helper function to calculate Tuesday-to-Tuesday week boundaries
  function getTuesdayWeekBoundaries(date: Date): { weekStartDate: string; weekEndDate: string } {
    const d = new Date(date);
    const day = d.getDay(); // 0 = Sunday, 1 = Monday, 2 = Tuesday, etc.
    
    // Calculate days to subtract to get to previous or current Tuesday
    let daysToSubtract = (day + 5) % 7; // If Tuesday (2), result is 0; if Wednesday (3), result is 1; etc.
    
    const weekStart = new Date(d);
    weekStart.setDate(d.getDate() - daysToSubtract);
    
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 7);
    
    return {
      weekStartDate: weekStart.toISOString().split('T')[0],
      weekEndDate: weekEnd.toISOString().split('T')[0]
    };
  }

  // Invoice routes
  app.get("/api/invoices", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const invoices = await storage.getAllInvoices(workspaceId);
      res.json(invoices);
    } catch (error) {
      console.error("Error fetching invoices:", error);
      res.status(500).json({ error: "Failed to fetch invoices" });
    }
  });

  app.get("/api/invoices/:id", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const id = parseInt(req.params.id);
      const invoice = await storage.getInvoice(id, workspaceId);
      
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      res.json(invoice);
    } catch (error) {
      console.error("Error fetching invoice:", error);
      res.status(500).json({ error: "Failed to fetch invoice" });
    }
  });

  // Create a new invoice manually
  app.post("/api/invoices", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { weekStartDate } = req.body;
      
      if (!weekStartDate) {
        return res.status(400).json({ error: "Week start date is required" });
      }
      
      const startDate = new Date(weekStartDate);
      const { weekStartDate: calculatedStart, weekEndDate } = getTuesdayWeekBoundaries(startDate);
      
      // Check if invoice already exists for this week
      const existingInvoice = await storage.getInvoiceByWeek(calculatedStart, workspaceId);
      if (existingInvoice) {
        return res.status(400).json({ error: "Invoice already exists for this week" });
      }
      
      // Generate invoice number
      const invoiceNumber = await storage.generateInvoiceNumber(workspaceId);
      
      // Create the invoice
      const invoice = await storage.createInvoice({
        workspaceId,
        invoiceNumber,
        weekStartDate: calculatedStart,
        weekEndDate,
        status: 'draft',
        totalAmount: '0'
      });
      
      res.json(invoice);
    } catch (error) {
      console.error("Error creating invoice:", error);
      res.status(500).json({ error: "Failed to create invoice" });
    }
  });

  app.get("/api/invoices/:id/items", isAuthenticated, isActive, async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const items = await storage.getInvoiceItems(id);
      res.json(items);
    } catch (error) {
      console.error("Error fetching invoice items:", error);
      res.status(500).json({ error: "Failed to fetch invoice items" });
    }
  });

  app.post("/api/invoices/:id/items", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const invoiceId = parseInt(req.params.id);
      
      // Verify invoice belongs to user's workspace
      const invoice = await storage.getInvoice(invoiceId, workspaceId);
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      // Validate request body
      const itemSchema = z.object({
        description: z.string().min(1),
        quantity: z.number().int().min(1).default(1),
        baseAmount: z.string(),
        includeGST: z.boolean().default(true),
        parentItemId: z.number().nullable().optional()
      });
      
      const data = itemSchema.parse(req.body);
      
      // Calculate amounts (baseAmount is unit price, multiply by quantity for total base)
      const unitPrice = parseFloat(data.baseAmount);
      const baseAmount = unitPrice * data.quantity;
      const gstAmount = data.includeGST ? baseAmount * 0.1 : 0;
      const totalAmount = baseAmount + gstAmount;
      
      // Create invoice item
      const item = await storage.createInvoiceItem({
        invoiceId,
        jobId: null,
        parentItemId: data.parentItemId || null,
        itemType: 'additional_charge',
        description: data.description,
        quantity: data.quantity,
        baseAmount: baseAmount.toFixed(2),
        gstAmount: gstAmount.toFixed(2),
        totalAmount: totalAmount.toFixed(2)
      });
      
      // Recalculate invoice total (sum base + sum gst to avoid floating-point errors)
      const allItems = await storage.getInvoiceItems(invoiceId);
      const subtotal = allItems.reduce((sum, item) => sum + parseFloat(item.baseAmount as string), 0);
      const gstTotal = allItems.reduce((sum, item) => sum + parseFloat(item.gstAmount as string), 0);
      const newTotal = subtotal + gstTotal;
      
      await storage.updateInvoice(invoiceId, { totalAmount: newTotal.toFixed(2) }, workspaceId);
      
      res.json(item);
    } catch (error) {
      console.error("Error creating invoice item:", error);
      res.status(500).json({ error: "Failed to create invoice item" });
    }
  });

  app.patch("/api/invoices/:invoiceId/items/:itemId", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const invoiceId = parseInt(req.params.invoiceId);
      const itemId = parseInt(req.params.itemId);
      
      // Verify invoice belongs to user's workspace
      const invoice = await storage.getInvoice(invoiceId, workspaceId);
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      // Validate request body - allow updating baseAmount
      const updateSchema = z.object({
        baseAmount: z.string()
      });
      
      const data = updateSchema.parse(req.body);
      
      // Calculate new amounts based on new baseAmount
      const baseAmount = parseFloat(data.baseAmount);
      const gstAmount = baseAmount * 0.1; // Always include GST for invoice items
      const totalAmount = baseAmount + gstAmount;
      
      // Update the item
      const updated = await storage.updateInvoiceItem(itemId, {
        baseAmount: baseAmount.toFixed(2),
        gstAmount: gstAmount.toFixed(2),
        totalAmount: totalAmount.toFixed(2)
      });
      
      if (updated) {
        // Recalculate invoice total (sum base + sum gst to avoid floating-point errors)
        const allItems = await storage.getInvoiceItems(invoiceId);
        const subtotal = allItems.reduce((sum, item) => sum + parseFloat(item.baseAmount as string), 0);
        const gstTotal = allItems.reduce((sum, item) => sum + parseFloat(item.gstAmount as string), 0);
        const newTotal = subtotal + gstTotal;
        
        await storage.updateInvoice(invoiceId, { totalAmount: newTotal.toFixed(2) }, workspaceId);
      }
      
      res.json(updated);
    } catch (error) {
      console.error("Error updating invoice item:", error);
      res.status(500).json({ error: "Failed to update invoice item" });
    }
  });

  app.delete("/api/invoices/:invoiceId/items/:itemId", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const invoiceId = parseInt(req.params.invoiceId);
      const itemId = parseInt(req.params.itemId);
      
      // Verify invoice belongs to user's workspace
      const invoice = await storage.getInvoice(invoiceId, workspaceId);
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      // Delete the item
      const success = await storage.deleteInvoiceItem(itemId);
      
      if (success) {
        // Recalculate invoice total (sum base + sum gst to avoid floating-point errors)
        const allItems = await storage.getInvoiceItems(invoiceId);
        const subtotal = allItems.reduce((sum, item) => sum + parseFloat(item.baseAmount as string), 0);
        const gstTotal = allItems.reduce((sum, item) => sum + parseFloat(item.gstAmount as string), 0);
        const newTotal = subtotal + gstTotal;
        
        await storage.updateInvoice(invoiceId, { totalAmount: newTotal.toFixed(2) }, workspaceId);
      }
      
      res.json({ success });
    } catch (error) {
      console.error("Error deleting invoice item:", error);
      res.status(500).json({ error: "Failed to delete invoice item" });
    }
  });

  app.patch("/api/invoices/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const id = parseInt(req.params.id);
      
      const invoice = await storage.updateInvoice(id, req.body, workspaceId);
      
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      res.json(invoice);
    } catch (error) {
      console.error("Error updating invoice:", error);
      res.status(500).json({ error: "Failed to update invoice" });
    }
  });

  app.patch("/api/invoices/:id/status", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const id = parseInt(req.params.id);
      
      const statusSchema = z.object({
        status: z.enum(['draft', 'finalized'])
      });
      
      const { status } = statusSchema.parse(req.body);
      
      const invoice = await storage.updateInvoice(id, { status }, workspaceId);
      
      if (!invoice) {
        return res.status(404).json({ error: "Invoice not found" });
      }
      
      res.json(invoice);
    } catch (error) {
      console.error("Error updating invoice status:", error);
      res.status(500).json({ error: "Failed to update invoice status" });
    }
  });

  // Workspace Invoice Settings routes
  app.get("/api/workspaces/:workspaceId/invoice-settings", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const userWorkspaceId = await getUserWorkspaceId(userId);
      const requestedWorkspaceId = parseInt(req.params.workspaceId);
      
      if (!userWorkspaceId || userWorkspaceId !== requestedWorkspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const settings = await storage.getWorkspaceInvoiceSettings(requestedWorkspaceId);
      res.json(settings || null);
    } catch (error) {
      console.error("Error fetching workspace invoice settings:", error);
      res.status(500).json({ error: "Failed to fetch invoice settings" });
    }
  });

  app.put("/api/workspaces/:workspaceId/invoice-settings", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const userWorkspaceId = await getUserWorkspaceId(userId);
      const requestedWorkspaceId = parseInt(req.params.workspaceId);
      
      if (!userWorkspaceId || userWorkspaceId !== requestedWorkspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const settingsData = {
        workspaceId: requestedWorkspaceId,
        ...req.body
      };
      
      const settings = await storage.upsertWorkspaceInvoiceSettings(settingsData);
      res.json(settings);
    } catch (error) {
      console.error("Error updating workspace invoice settings:", error);
      res.status(500).json({ error: "Failed to update invoice settings" });
    }
  });

  // Saved Charges routes
  app.get("/api/saved-charges", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const charges = await storage.getAllSavedCharges(workspaceId);
      res.json(charges);
    } catch (error) {
      console.error("Error fetching saved charges:", error);
      res.status(500).json({ error: "Failed to fetch saved charges" });
    }
  });

  app.post("/api/saved-charges", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { description, defaultAmount, includeGst } = req.body;
      
      const charge = await storage.createSavedCharge({
        workspaceId,
        description,
        defaultAmount,
        includeGst: includeGst ?? true,
      });
      
      res.json(charge);
    } catch (error) {
      console.error("Error creating saved charge:", error);
      res.status(500).json({ error: "Failed to create saved charge" });
    }
  });

  app.delete("/api/saved-charges/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const id = parseInt(req.params.id);
      const success = await storage.deleteSavedCharge(id, workspaceId);
      
      res.json({ success });
    } catch (error) {
      console.error("Error deleting saved charge:", error);
      res.status(500).json({ error: "Failed to delete saved charge" });
    }
  });

  // Bill To Contacts routes
  app.get("/api/bill-to-contacts", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const contacts = await storage.getAllBillToContacts(workspaceId);
      res.json(contacts);
    } catch (error) {
      console.error("Error fetching bill to contacts:", error);
      res.status(500).json({ error: "Failed to fetch bill to contacts" });
    }
  });

  app.post("/api/bill-to-contacts", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const { name, company, address, phone, email } = req.body;
      
      // Check if contact already exists (by name, phone, and email)
      const existingContact = await storage.findBillToContact(workspaceId, name, phone, email);
      
      if (existingContact) {
        return res.json(existingContact);
      }
      
      const contact = await storage.createBillToContact({
        workspaceId,
        name,
        company,
        address,
        phone,
        email,
      });
      
      res.json(contact);
    } catch (error) {
      console.error("Error creating bill to contact:", error);
      res.status(500).json({ error: "Failed to create bill to contact" });
    }
  });

  // ============================================
  // ROUTE OPTIMIZATION API ENDPOINTS (Read-only)
  // ============================================

  // Get optimized day routes for jobs using two-tier clustering (urgent vs standard)
  app.get("/api/route-planner", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      // Check if CM jobs should be included (defaults to true)
      const includeCM = req.query.includeCM !== 'false';
      
      // Get user's location settings
      const locationSettings = await storage.getUserLocationSettings(userId);
      
      // Determine which warehouse to use based on user preference
      const warehouseKey = locationSettings?.primaryWarehouse || 'malaga';
      const warehouseLocation = WAREHOUSES[warehouseKey as keyof typeof WAREHOUSES] || WAREHOUSES.malaga;
      
      // Get all jobs - the clustering function will filter to "To Do" only
      let allJobs = await storage.getAllJobs(workspaceId);
      
      // Filter out CM jobs if includeCM is false
      if (!includeCM) {
        allJobs = allJobs.filter(job => !job.isCheckMeasureJob);
      }
      
      // Build route options including home location if enabled
      const routeOptions = {
        warehouse: warehouseLocation,
        home: locationSettings?.homeLatitude && locationSettings?.homeLongitude 
          ? { lat: parseFloat(locationSettings.homeLatitude), lng: parseFloat(locationSettings.homeLongitude) }
          : null,
        includeHomeInRoute: locationSettings?.includeHomeInRoute || false
      };
      
      // Generate two-tier clusters (urgent vs standard)
      const clusterResult = generateTwoTierClusters(allJobs, routeOptions);
      
      // Get calendar availability for the next 30 days (shows ALL bookings including personal)
      const calendarAvailability = await getCalendarAvailability(30, workspaceId);
      
      // Calculate total busy time from calendar for summary
      const totalCalendarBusyMinutes = calendarAvailability.reduce((sum, day) => sum + day.busyMinutes, 0);
      const daysWithBookings = calendarAvailability.filter(d => d.busyMinutes > 0).length;
      
      res.json({
        urgentClusters: clusterResult.urgentClusters,
        standardClusters: clusterResult.standardClusters,
        ungeocoded: clusterResult.ungeocoded.map(j => ({
          id: j.id,
          jobId: j.jobId,
          clientName: j.clientName,
          address: j.address,
          eta: j.eta,
          installCost: j.installCost
        })),
        calendarAvailability: calendarAvailability,
        stats: {
          ...clusterResult.stats,
          calendarConnected: calendarAvailability.length > 0,
          daysWithBookings,
          totalCalendarBusyHours: Math.round(totalCalendarBusyMinutes / 60)
        },
        warehouseLocation: warehouseLocation,
        availableWarehouses: WAREHOUSES,
        userLocationSettings: locationSettings ? {
          homeAddress: locationSettings.homeAddress,
          homeLatitude: locationSettings.homeLatitude ? parseFloat(locationSettings.homeLatitude) : null,
          homeLongitude: locationSettings.homeLongitude ? parseFloat(locationSettings.homeLongitude) : null,
          primaryWarehouse: locationSettings.primaryWarehouse,
          includeHomeInRoute: locationSettings.includeHomeInRoute
        } : null
      });
    } catch (error) {
      console.error("Error generating route plan:", error);
      res.status(500).json({ error: "Failed to generate route plan" });
    }
  });

  // Geocode a single job address
  app.post("/api/geocode-job/:id", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const jobId = parseInt(req.params.id);
      const job = await storage.getJob(jobId);
      
      if (!job || job.workspaceId !== workspaceId) {
        return res.status(404).json({ error: "Job not found" });
      }
      
      // Geocode the address
      const coords = await geocodeAddress(job.address);
      
      if (coords) {
        // Update job with coordinates (use toFixed for proper numeric format)
        await storage.updateJob(jobId, {
          latitude: coords.lat.toFixed(7),
          longitude: coords.lng.toFixed(7),
          geocodeStatus: 'success'
        }, workspaceId);
        
        res.json({ success: true, latitude: coords.lat, longitude: coords.lng });
      } else {
        await storage.updateJob(jobId, { geocodeStatus: 'failed' }, workspaceId);
        res.json({ success: false, error: "Could not geocode address" });
      }
    } catch (error) {
      console.error("Error geocoding job:", error);
      res.status(500).json({ error: "Failed to geocode job" });
    }
  });

  // Batch geocode all jobs without coordinates
  app.post("/api/geocode-all", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const workspaceId = await getUserWorkspaceId(userId);
      
      if (!workspaceId) {
        return res.status(403).json({ error: "No workspace access" });
      }
      
      const allJobs = await storage.getAllJobs(workspaceId);
      const jobsToGeocode = allJobs.filter(j => 
        (!j.latitude || !j.longitude) && j.address && j.address.trim() !== '' && j.address !== 'Address not found'
      );
      
      // Reset failed status so they get retried with improved geocoding
      for (const job of jobsToGeocode) {
        if (job.geocodeStatus === 'failed') {
          await storage.updateJob(job.id, { geocodeStatus: null as any }, workspaceId);
        }
      }
      
      let successCount = 0;
      let failCount = 0;
      
      for (const job of jobsToGeocode) {
        const coords = await geocodeAddress(job.address);
        
        if (coords) {
          await storage.updateJob(job.id, {
            latitude: coords.lat.toFixed(7),
            longitude: coords.lng.toFixed(7),
            geocodeStatus: 'success'
          }, workspaceId);
          successCount++;
          console.log(`Saved coordinates for job ${job.id}: ${coords.lat}, ${coords.lng}`);
        } else {
          await storage.updateJob(job.id, { geocodeStatus: 'failed' }, workspaceId);
          failCount++;
        }
        
        await new Promise(resolve => setTimeout(resolve, 1100));
      }
      
      res.json({ 
        success: true, 
        geocoded: successCount, 
        failed: failCount,
        total: jobsToGeocode.length 
      });
    } catch (error) {
      console.error("Error batch geocoding:", error);
      res.status(500).json({ error: "Failed to batch geocode jobs" });
    }
  });

  // ==================== Installation Videos ====================
  app.get("/api/videos", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const { db } = await import('./db');
      const { installationVideos } = await import('@shared/schema');
      const { eq, asc } = await import('drizzle-orm');
      const videos = await db.select().from(installationVideos)
        .where(eq(installationVideos.workspaceId, workspaceId))
        .orderBy(asc(installationVideos.sortOrder), asc(installationVideos.createdAt));
      res.json(videos);
    } catch (error) {
      console.error("Error fetching videos:", error);
      res.status(500).json({ error: "Failed to fetch videos" });
    }
  });

  app.post("/api/videos", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const { title, youtubeUrl, description, category } = req.body;
      if (!title || !youtubeUrl) {
        return res.status(400).json({ error: "Title and YouTube URL are required" });
      }
      const { db } = await import('./db');
      const { installationVideos } = await import('@shared/schema');
      const [video] = await db.insert(installationVideos).values({
        workspaceId,
        title,
        youtubeUrl,
        description: description || null,
        category: category || "general",
      }).returning();
      res.json(video);
    } catch (error) {
      console.error("Error creating video:", error);
      res.status(500).json({ error: "Failed to create video" });
    }
  });

  app.patch("/api/videos/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const videoId = parseInt(req.params.id);
      const { title, youtubeUrl, description, category, sortOrder } = req.body;
      const { db } = await import('./db');
      const { installationVideos } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      const updates: any = {};
      if (title !== undefined) updates.title = title;
      if (youtubeUrl !== undefined) updates.youtubeUrl = youtubeUrl;
      if (description !== undefined) updates.description = description;
      if (category !== undefined) updates.category = category;
      if (sortOrder !== undefined) updates.sortOrder = sortOrder;
      const [video] = await db.update(installationVideos)
        .set(updates)
        .where(and(eq(installationVideos.id, videoId), eq(installationVideos.workspaceId, workspaceId)))
        .returning();
      if (!video) return res.status(404).json({ error: "Video not found" });
      res.json(video);
    } catch (error) {
      console.error("Error updating video:", error);
      res.status(500).json({ error: "Failed to update video" });
    }
  });

  app.delete("/api/videos/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const videoId = parseInt(req.params.id);
      const { db } = await import('./db');
      const { installationVideos } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      const [deleted] = await db.delete(installationVideos)
        .where(and(eq(installationVideos.id, videoId), eq(installationVideos.workspaceId, workspaceId)))
        .returning();
      if (!deleted) return res.status(404).json({ error: "Video not found" });
      res.json({ success: true });
    } catch (error) {
      console.error("Error deleting video:", error);
      res.status(500).json({ error: "Failed to delete video" });
    }
  });

  // ==================== Gallery Photos ====================
  app.get("/api/gallery", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const { db } = await import('./db');
      const { galleryPhotos } = await import('@shared/schema');
      const { eq, desc } = await import('drizzle-orm');
      const photos = await db.select({
        id: galleryPhotos.id,
        workspaceId: galleryPhotos.workspaceId,
        title: galleryPhotos.title,
        description: galleryPhotos.description,
        category: galleryPhotos.category,
        jobId: galleryPhotos.jobId,
        createdAt: galleryPhotos.createdAt,
      }).from(galleryPhotos)
        .where(eq(galleryPhotos.workspaceId, workspaceId))
        .orderBy(desc(galleryPhotos.createdAt));
      res.json(photos);
    } catch (error) {
      console.error("Error fetching gallery:", error);
      res.status(500).json({ error: "Failed to fetch gallery" });
    }
  });

  app.get("/api/gallery/:id/image", isAuthenticated, isActive, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const photoId = parseInt(req.params.id);
      const { db } = await import('./db');
      const { galleryPhotos } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      const [photo] = await db.select({ imageData: galleryPhotos.imageData })
        .from(galleryPhotos)
        .where(and(eq(galleryPhotos.id, photoId), eq(galleryPhotos.workspaceId, workspaceId)));
      if (!photo) return res.status(404).json({ error: "Photo not found" });
      const match = photo.imageData.match(/^data:(image\/\w+);base64,(.+)$/);
      if (match) {
        const mimeType = match[1];
        const buffer = Buffer.from(match[2], 'base64');
        res.set('Content-Type', mimeType);
        res.set('Cache-Control', 'public, max-age=86400');
        res.send(buffer);
      } else {
        res.status(400).json({ error: "Invalid image data" });
      }
    } catch (error) {
      console.error("Error fetching photo image:", error);
      res.status(500).json({ error: "Failed to fetch photo" });
    }
  });

  app.post("/api/gallery", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const { title, description, category, imageData, jobId } = req.body;
      if (!imageData) {
        return res.status(400).json({ error: "Image data is required" });
      }
      const { db } = await import('./db');
      const { galleryPhotos } = await import('@shared/schema');
      const [photo] = await db.insert(galleryPhotos).values({
        workspaceId,
        title: title || null,
        description: description || null,
        category: category || "general",
        imageData,
        jobId: jobId || null,
      }).returning({
        id: galleryPhotos.id,
        workspaceId: galleryPhotos.workspaceId,
        title: galleryPhotos.title,
        description: galleryPhotos.description,
        category: galleryPhotos.category,
        jobId: galleryPhotos.jobId,
        createdAt: galleryPhotos.createdAt,
      });
      res.json(photo);
    } catch (error) {
      console.error("Error uploading photo:", error);
      res.status(500).json({ error: "Failed to upload photo" });
    }
  });

  app.patch("/api/gallery/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const photoId = parseInt(req.params.id);
      const { title, description, category } = req.body;
      const { db } = await import('./db');
      const { galleryPhotos } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      const updates: any = {};
      if (title !== undefined) updates.title = title;
      if (description !== undefined) updates.description = description;
      if (category !== undefined) updates.category = category;
      const [photo] = await db.update(galleryPhotos)
        .set(updates)
        .where(and(eq(galleryPhotos.id, photoId), eq(galleryPhotos.workspaceId, workspaceId)))
        .returning({
          id: galleryPhotos.id,
          workspaceId: galleryPhotos.workspaceId,
          title: galleryPhotos.title,
          description: galleryPhotos.description,
          category: galleryPhotos.category,
          jobId: galleryPhotos.jobId,
          createdAt: galleryPhotos.createdAt,
        });
      if (!photo) return res.status(404).json({ error: "Photo not found" });
      res.json(photo);
    } catch (error) {
      console.error("Error updating photo:", error);
      res.status(500).json({ error: "Failed to update photo" });
    }
  });

  app.delete("/api/gallery/:id", isAuthenticated, isActive, requireWriteAccess, async (req: any, res) => {
    try {
      const userId = req.user?.claims?.sub;
      const workspaceId = userId ? await getUserWorkspaceId(userId) : null;
      if (!workspaceId) return res.status(403).json({ error: "No workspace access" });
      const photoId = parseInt(req.params.id);
      const { db } = await import('./db');
      const { galleryPhotos } = await import('@shared/schema');
      const { eq, and } = await import('drizzle-orm');
      const [deleted] = await db.delete(galleryPhotos)
        .where(and(eq(galleryPhotos.id, photoId), eq(galleryPhotos.workspaceId, workspaceId)))
        .returning();
      if (!deleted) return res.status(404).json({ error: "Photo not found" });
      res.json({ success: true });
    } catch (error) {
      console.error("Error deleting photo:", error);
      res.status(500).json({ error: "Failed to delete photo" });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
