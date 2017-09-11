package com.axibase.tsd.driver.jdbc.util;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class WildcardsUtil {
	private static final char ONE_ANY_SYMBOL = '_';
	private static final char NONE_OR_MORE_SYMBOLS = '%';
	private static final char ATSD_ONE_ANY_SYMBOL = '?';
	private static final char ATSD_NONE_OR_MORE_SYMBOLS = '*';
	private static final char ESCAPE_CHAR = '\\';
	private static final int NOT_FOUND = -1;

	private static final Pattern SQL_WILDCARDS_PATTERN = Pattern.compile("(?<!\\\\)[%_]");

	public static boolean hasWildcards(String text) {
		return text == null || SQL_WILDCARDS_PATTERN.matcher(text).find();
	}

	private static boolean hasWildcards(String text, char oneSymbolWildcard, char manySymbolsWildcard) {
		return text == null || text.indexOf(oneSymbolWildcard) != NOT_FOUND || text.indexOf(manySymbolsWildcard) != NOT_FOUND;
	}

	public static boolean isRetrieveAllPattern(String text) {
		return text == null || (text.length() == 1 && text.charAt(0) == NONE_OR_MORE_SYMBOLS);
	}

	public static boolean wildcardMatch(String text, String pattern) {
		return wildcardMatch(text, pattern, ONE_ANY_SYMBOL, NONE_OR_MORE_SYMBOLS);
	}

	private static boolean wildcardMatch(String text, String pattern, char anySymbol, char manySymbol) {
		if (pattern == null) {
			return true;
		}
		if (text == null) {
			return false;
		}

		final String oneAnySymbolStr = String.valueOf(anySymbol);
		final String noneOrMoreSymbolsStr = String.valueOf(manySymbol);
		final int stringLength = text.length();
		final String[] wildcardTokens = splitOnTokens(pattern, oneAnySymbolStr, noneOrMoreSymbolsStr, anySymbol, manySymbol);
		boolean anyChars = false;
		int textIdx = 0;
		int wildcardTokensIdx = 0;
		final List<BacktrackContext> backtrack = new ArrayList<>();

		// loop around a backtrack stack to handle complex % matching
		do {
			final int backtrackListSize = backtrack.size();
			if (backtrackListSize > 0) {
				BacktrackContext context = backtrack.remove(backtrackListSize - 1);
				wildcardTokensIdx = context.tokenIndex;
				textIdx = context.charIndex;
				anyChars = true;
			}

			// loop whilst tokens and text left to process
			while (wildcardTokensIdx < wildcardTokens.length) {
				final String wildcardToken = wildcardTokens[wildcardTokensIdx];
				if (oneAnySymbolStr.equals(wildcardToken)) {
					// found one-symbol mask, hence move to next text char
					++textIdx;
					if (textIdx > stringLength) {
						break;
					}
					anyChars = false;
				} else if (noneOrMoreSymbolsStr.equals(wildcardToken)) {
					anyChars = true;
					if (wildcardTokensIdx == wildcardTokens.length - 1) {
						textIdx = stringLength;
					}
				} else {
					if (anyChars) {
						// any chars, hence try to locate text token
						textIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx);
						if (textIdx == NOT_FOUND) {
							break;
						}
						int repeatIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx  +1);
						if (repeatIdx >= 0) {
							backtrack.add(new BacktrackContext(wildcardTokensIdx, repeatIdx));
						}
					} else {
						// matching from current position
						if (!text.regionMatches(true, textIdx, wildcardToken, 0, wildcardToken.length())) {
							// couldn't match token
							break;
						}
					}

					// matched text token, move text index to end of matched token
					textIdx += wildcardToken.length();
					anyChars = false;
				}

				++wildcardTokensIdx;
			}

			// full match
			if (wildcardTokensIdx == wildcardTokens.length && textIdx == text.length()) {
				return true;
			}

		} while (!backtrack.isEmpty());

		return false;
	}

	/**
	 * Splits a string into a number of tokens.
	 * The text is split by '_' and '%'.
	 * Multiple '%' are collapsed into a single '%', patterns like "%_" will be transferred to "_%"
	 *
	 * @param text  the text to split
	 * @return the array of tokens, never null
	 */
	private static String[] splitOnTokens(String text, String oneAnySymbolStr, String noneOrMoreSymbolsStr, char oneAnySymbol, char noneOrMoreSymbols) {
		if (!hasWildcards(text, oneAnySymbol, noneOrMoreSymbols)) {
			return new String[] { text };
		}
		List<String> list = new ArrayList<>();
		StringBuilder buffer = new StringBuilder();

		final int length = text.length();
		for (int i = 0; i < length; i++) {
			final char current = text.charAt(i);

			if (current == oneAnySymbol) {
				flushBuffer(buffer, list);
				if (!list.isEmpty() && noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
					list.set(list.size() - 1, oneAnySymbolStr);
					list.add(noneOrMoreSymbolsStr);
				} else {
					list.add(oneAnySymbolStr);
				}
			} else if (current == noneOrMoreSymbols) {
				flushBuffer(buffer, list);
				if (list.isEmpty() || !noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
					list.add(noneOrMoreSymbolsStr);
				}
			} else {
				buffer.append(current);
			}
		}
		if (buffer.length() != 0) {
			list.add(buffer.toString());
		}

		return list.toArray(new String[list.size()]);
	}

	public static String replaceSqlWildcardsWithAtsdUseEscaping(String text) {
		if (StringUtils.isEmpty(text)) {
			return text;
		}
		boolean modified = false;
		int newStrIndex = 0;
		final char[] chars = text.toCharArray();
		char[] newStrChars = chars;
		for (final char symbol : chars) {
			switch (symbol) {
				case ONE_ANY_SYMBOL:
					newStrChars[newStrIndex] = ATSD_ONE_ANY_SYMBOL;
					modified = true;
					break;
				case NONE_OR_MORE_SYMBOLS:
					newStrChars[newStrIndex] = ATSD_NONE_OR_MORE_SYMBOLS;
					modified = true;
					break;
				case ATSD_ONE_ANY_SYMBOL:
					newStrChars = initNewStringCharArray(chars, newStrChars, newStrIndex);
					newStrChars[newStrIndex++] = ESCAPE_CHAR;
					newStrChars[newStrIndex] = ATSD_ONE_ANY_SYMBOL;
					modified = true;
					break;
				case ATSD_NONE_OR_MORE_SYMBOLS:
					newStrChars = initNewStringCharArray(chars, newStrChars, newStrIndex);
					newStrChars[newStrIndex++] = ESCAPE_CHAR;
					newStrChars[newStrIndex] = ATSD_NONE_OR_MORE_SYMBOLS;
					modified = true;
					break;
				default:
					newStrChars[newStrIndex] = symbol;
			}
			++newStrIndex;
		}

		return modified ? new String(newStrChars, 0, newStrIndex) : text;

	}

	private static char[] initNewStringCharArray(char[] old, char[] newArray, int index) {
		if (old == newArray) {
			newArray = new char[old.length * 2 - index];
			System.arraycopy(old, 0, newArray, 0, index);
		}
		return newArray;
	}

	private static void flushBuffer(StringBuilder buffer, List<String> list) {
		if (buffer.length() != 0) {
			list.add(buffer.toString());
			buffer.setLength(0);
		}
	}

	@AllArgsConstructor
	private static final class BacktrackContext {
		private final int tokenIndex;
		private final int charIndex;
	}

}
